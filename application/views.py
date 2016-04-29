from flask import render_template, request, redirect
from application import app, models
from application.db import plots
from worker import conn
from rq import Queue


@app.route('/')
def main():
    return redirect('/index')


@app.route('/index', methods=['GET', 'POST'])
def index():
    params = {}

    return render_template('index.html', params=params)


@app.route('/test', methods=['GET', 'POST'])
def test():
    if request.method == 'POST':
        forms = request.form
        print forms

    return render_template('test.html')


@app.route('/models.html')
def redirect_model():
    return redirect('/models')


@app.route('/models', methods=['GET', 'POST'])
def model():
    if request.method == 'POST':
        forms = request.form
        name = forms['game']
    else:
        name = 'League of Legends'

    div, stream = models.create_stream(name)
    params = dict(game=name, div=div)

    queue = Queue(connection=conn)
    queue.enqueue(plots.stream_model_data, stream)

    return render_template('models.html', params=params)
