from flask import render_template, request, redirect
from application import app, models


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
        name = request.form['game']
        if models.check_list(name):
            div = models.create_stream(name)

        else:
            name = 'League of Legends'
            div = '<div class="error"><h3>No Game Found</h3></div>'

        params = dict(game=name, div=div)
        return render_template('models.html', params=params)
    else:
        name = 'League of Legends'
        div = '<iframe id="igraph" scrolling="no" style="border:none;" seamless="seamless" src="https://plot.ly/~styoung/94.embed" height="525" width="100%"></iframe>'


        params = dict(game=name, div=div)
        return render_template('models.html', params=params)