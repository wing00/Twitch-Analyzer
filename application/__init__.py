from flask import Flask
from flask_bootstrap import Bootstrap

app = Flask(__name__)
Bootstrap(app)

app.config.from_pyfile('settings.cfg')
app.debug = app.config['DEBUG']

from application import views

if __name__ == '__main__':
    app.run()

