import os
from logging import DEBUG

from flask import Flask
from flask_login import LoginManager
from flask_sqlalchemy import SQLAlchemy


basedir = os.path.abspath(os.path.dirname(__file__))
app = Flask(__name__)
app.config[
    'SECRET_KEY'] = '\xe6\xfc\xea\xb9a\x8b\x13\x10\x88\x08Pu\xf9\xf2\xb09\xffx\xfcftj\xf3\x04'
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + os.path.join(basedir,
                                                                    'olapy.db')
app.config['DEBUG'] = True
app.logger.setLevel(DEBUG)
db = SQLAlchemy(app)
login_manager = LoginManager()
login_manager.session_protection = 'strong'
login_manager.login_view = 'login'
login_manager.init_app(app)

# this import should at the bottom (app is used in views module)
import views
