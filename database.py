import sqlalchemy
import logging


logging.getLogger().setLevel(logging.INFO)

def check_version():
    logging.info(sqlalchemy.__version__)

def main():
    check_version()

if __name__ == '__main__':
    main()