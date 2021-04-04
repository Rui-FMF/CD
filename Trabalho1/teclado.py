import sys
import fcntl
import os
import selectors

# set sys.stdin non-blocking
orig_fl = fcntl.fcntl(sys.stdin, fcntl.F_GETFL)
fcntl.fcntl(sys.stdin, fcntl.F_SETFL, orig_fl | os.O_NONBLOCK)

# function to be called when enter is pressed
def got_keyboard_data(stdin):
    data = stdin.read()
    if data in '\r\n':
        sys.exit(0)
    print(f'Keyboard input: {data}', end='')

# register event
selector = selectors.DefaultSelector()
selector.register(sys.stdin, selectors.EVENT_READ, got_keyboard_data)

while True:
    print('Type something and hit enter: ', end='', flush=True)
    for k, mask in selector.select():
        callback = k.data
        callback(k.fileobj)