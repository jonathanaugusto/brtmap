from flask import Flask, redirect, url_for, request, render_template
from Queries import heatMapQuery, brtPosQuery
import json
app = Flask(__name__)

@app.route('/')
def main_page():
    try:
        return render_template('index.html')
    except Exception as e:
        return str(e)

@app.route('/brtpos', methods=['GET', 'POST'])
def brtpos():
    try:
        linha = ""
        if request.method == 'GET':
            linha = request.args.get('linha')
        else:
            linha = request.form.get('linha')
        
        result = brtPosQuery(linha)
        
        return json.dumps(result)
    except Exception as e:
        return str(e)

@app.route('/heatMap', methods=['GET', 'POST'])
def heatMap():
    try:
        linha = ""
        if request.method == 'GET':
            linha = request.args.get('linha')
        else:
            linha = request.form.get('linha')
        
        result = heatMapQuery(linha)
        
        return json.dumps(result)
    except Exception as e:
        return str(e)
    
@app.route('/how_to_use')
def how_to_use():
    return "Explanation"

if __name__ == '__main__':

    app.run(host="127.0.0.1", port=5555)

