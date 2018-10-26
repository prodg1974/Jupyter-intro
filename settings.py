import os
from os.path import join, dirname
from dotenv import load_dotenv

basedir = os.path.abspath(os.path.dirname(__file__))
dotenv_path = join(dirname(__file__),'.env')
load_dotenv(dotenv_path)



class Config(object):
    os.path.join(basedir,'app.db')
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'you-will-never-guess'
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL') or'sqlite:///'
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    DATABASE_URL = os.environ.get("DATABASE_URL")

    SQL_LOGIN = os.environ.get("SQL_LOGIN")
    SQL_PWD = os.environ.get("SQL_PWD")

    CMP_LOGIN = os.environ.get("CMP_LOGIN")
    CMP_PWD = os.environ.get("CMP_PWD")
    CMP_KEY = os.environ.get("CMP_KEY")

    TEST_PORT=os.environ.get("TEST_PORT")
