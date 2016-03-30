from flask import Flask

app = Flask(__name__)
app.config.from_pyfile('settings.cfg')
app.debug = app.config['DEBUG']

from application import views
