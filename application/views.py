from flask import render_template, request, redirect
from application import app


@app.route('/')
def main():
    return redirect('/index')


@app.route('/index', methods=['GET', 'POST'])
def index():
    params = {}

    return render_template('index.html', params=params)


@app.route('/test')
def test():
    return render_template('test.html')

@app.route('/model')
def model():
    return render_template('model.html')
