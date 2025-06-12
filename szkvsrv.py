from flask import Flask, request, jsonify
import psycopg2

app = Flask(__name__)

conn = psycopg2.connect( 
    dbname='cafe',
    user='cafeadmin',
    password='szk',
    host='localhost',
    port='5432'
)

cursor = conn.cursor()

@app.route('/szkv/stats', methods=['GET'])
def get_stats():
    cons_id = int(request.args.get('pin'))
    try:
        cursor.execute("SELECT name FROM cf.consumers WHERE id = %s", (cons_id,))
        consumer = cursor.fetchone()
        name = consumer[0] if consumer else 'Unknown Consumer'
        return jsonify(name), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    

if __name__ == '__main__':
    app.run(host='localhost', port=5000, debug=True)    

