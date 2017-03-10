from __future__ import absolute_import, division, print_function

from flask_script import Manager, prompt_bool

from olapy.web import app, db
from olapy.web.models import User

manager = Manager(app)


@manager.command
def initdb():
    db.create_all()
    db.session.add(
        User(
            username="admin", email="admin@admin.com", password='admin'))
    db.session.add(
        User(
            username="demo", email="demo@demo.com", password="demo"))
    db.session.commit()
    print('Initialized the database')


@manager.command
def dropdb():
    if prompt_bool('Are you sure you want to lose all your data? '):
        db.drop_all()
        print('Dropped the database')


if __name__ == '__main__':
    manager.run()
