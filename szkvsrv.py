from flask import Flask, request, jsonify
import psycopg2
import threading
import time
from datetime import datetime, timedelta

app = Flask(__name__)

DB_CONFIG = { 
    'dbname': 'cafe',
    'user': 'cafeadmin',
    'password': 'szk',
    'host': 'localhost',
    'port': '5432'
}

def fetch_one(query, params):
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                result = cursor.fetchone()
                return result[0] if result else None
    except Exception as e:
        raise RuntimeError(f"Database error: {str(e)}")

def fetch_all(query, params = None):
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                result = cursor.fetchall()
                return result if result else None
    except Exception as e:
        raise RuntimeError(f"Database error: {str(e)}")



def insert_one(query, params):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                conn.commit()
                log_insert(f"{timestamp} - Inserted: {params} \n")
                return jsonify({"status": "success"}), 200
    except Exception as e:
        log_insert(f"{timestamp} - Insert error: {str(e)} \n")
        raise RuntimeError(f"Insert error: {str(e)}")    
    
def log_insert(text):
    try:
        with open("szkvsrv.log", "a", encoding="utf-8") as log_file:
            log_file.write( text )
    except Exception as e:
        print(f"Log error: {str(e)}")



req_list = []
cons_ids = set()
lock = threading.Lock()

def req_add(item):
    with lock:
        if item["id"] not in cons_ids:
            item["time"] = datetime.now()
            req_list.append(item)
            cons_ids.add(item["id"])

def req_remove(item_del):            
    with lock:
        for i, item in enumerate(req_list):
            if item["id"] == item_del["id"]:
                    del req_list[i]
                    cons_ids.discard(item["id"])
                    break

# request list maintenance thread
def req_maintenance():
    while True:
        time.sleep(3)
        with lock:
            now = datetime.now()
            new_req_list = []
            new_cons_ids = set()
            for item in req_list:    
                if  now - item["time"] < timedelta(seconds=10):
                    new_req_list.append(item)
                    new_cons_ids.add(item["id"])
            req_list.clear()
            req_list.extend(new_req_list)
            cons_ids.clear()
            cons_ids.update(new_cons_ids)
            

# Start the maintenance thread
threading.Thread(target=req_maintenance, daemon=True).start()


@app.route('/consumer_data', methods=["GET"])
def get_stats():
    
    cons_id = request.args.get("pin")
    try:
        name = fetch_one("SELECT name FROM cf.consumers WHERE id = %s AND active", (cons_id,))
        consumptions = fetch_one("SELECT count(*) FROM cf.cups WHERE consumer_id = %s", (cons_id,))
        cons_payable = fetch_one("SELECT count(*) FROM cf.cups WHERE consumer_id = %s AND NOT paid", (cons_id,))
        payable = fetch_one("SELECT payable FROM cf.consumers WHERE id = %s", (cons_id,))
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
    consumptions = consumptions if consumptions is not None else 0
    cons_payable = cons_payable if cons_payable is not None else 0
    payable = payable if payable is not None else 0

    print(name)

    if name is not None:
        return  jsonify({"name": name,
                    "consumptions": consumptions,
                    "cons_payable": cons_payable,
                     "payable": payable}), 200
    else:
        return jsonify({'error': 'Invalid pin or inactive user!'}), 500

@app.route('/request_coffee', methods=["POST"])    
def req_coffee():
    data = request.get_json()
    cons_id = data.get("pin")
    bag_id = fetch_one("SELECT id FROM cf.bags WHERE end_date IS NULL", ())
    if not bag_id:
        return jsonify({"error": "Nincs megkezdett csomag!"}), 400
    if cons_id:
        req_add({"id": cons_id, "time": ""})
        return jsonify({"status": "success"}), 200
    else:
        return jsonify({"error": "Invalid data!"}), 400
    

@app.route('/confirm_coffee_request', methods=["POST"])    
def confirm_coffee():
    data = request.get_json()
    cons_id = data.get("pin")
    find_id = next((item for item in req_list if item["id"] == cons_id), False)
    if not find_id:
        fetch_one("SELECT id FROM cf.consumers WHERE id = 0", ())        
        return jsonify({"error": "Nincs még kávéigény rögzítve!"}), 400
    bag_id = fetch_one("SELECT id FROM cf.bags WHERE end_date IS NULL", ())
    date_now = datetime.now().strftime('%Y-%m-%d')
    time_now = datetime.now().strftime('%H:%M:%S')
    insert_one(
        "INSERT INTO cf.cups (consumer_id, bag_id, c_date, c_time, paid) VALUES (%s, %s, %s, %s, %s)",
        (cons_id, bag_id, date_now, time_now, False)
    )
    req_remove({"id": cons_id, "time": ""})
    return jsonify({"status": "success"}), 200

@app.route('/consumption_data', methods=["GET"])
def get_cups():
    
    try:
        rows = fetch_all("select co.name, ba.id, ba.brand, c_date, c_time from cf.cups as cu " \
                            "join cf.bags as ba on cu.bag_id = ba.id" \
                            " join cf.consumers as co on cu.consumer_id = co.id " \
                            " order by c_date desc, c_time desc limit 100", None)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
    data = [
        {
            "name": r[0],
            "bag_id": r[1],
            "brand": r[2],
            "date": r[3].isoformat(),  # date -> ISO string
            "time": str(r[4])          # time -> string
        }
        for r in rows
    ]
   
    return jsonify({"status": "success", "data": data}), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, ssl_context=(
        '/home/al/Python/SZKV_SRV/cert/cert.pem',
        '/home/al/Python/SZKV_SRV/cert/key.pem'),
         debug=True)    

