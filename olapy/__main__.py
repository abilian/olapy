from __future__ import absolute_import, division, print_function

import sys
from core.services.xmla import start_server


def main(arg):
    '''
    Execute xmla provider
    :param arg: -c | --console :  show logs in server console
    :return:
    '''
    if len(arg) > 1:
        if arg[1] in ("-c", "--console"):
            start_server(write_on_file=False)
        else:
            print('invalide argument !')
    else:
        start_server(write_on_file=True)


if __name__ == "__main__":
    main(sys.argv)
