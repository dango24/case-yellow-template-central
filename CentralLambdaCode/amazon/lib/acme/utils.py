from subprocess import Popen, PIPE
from datetime import datetime,timedelta

def validate_runfile(runfile):
    """
    validate the runfile json data structure parsed from run file.
    If this run file indicates the correct user and command, it's valid. Otherwise, it's not.
    :param runfile: A json object
    :return: boolean
    """

    if_valid = True

    user = None
    pid = None
    try:
        user = runfile['user']  # get the user indicated in the run file
        pid = runfile['pid']  # get the pid indicated in the run file
    except KeyError as exp:
        if_valid = False

    if not user or not pid:
        if_valid = False

    if if_valid:
        pid_string = str(pid)
        p = Popen(['ps', "-p", pid_string, "-o", "user,cmd", "h"],
                  stdout=PIPE)  # run ps command to get the real info depending on the pid
        text = p.stdout.read()
        text = text.strip().split(' ', 1)

        if len(text[0]) == 0:  # ps command returns nothing, this means this PID is invalid.
            if_valid = False
        else:
            print text
            real_user = text[0]  # under this PID, the actual user of this process
            real_cmd = text[1]  # under this PID, the actual program of this process
            if (real_user != user) or ("python" not in real_cmd.lower()) or ("acmed" not in real_cmd.lower()):
                if_valid = False

    return if_valid

def dt_parse(t):
    """
    Converts %Y-%m-%d %H:%M:%S%z format to datetime object. 
    Python 2.7 does not support %z datetime format  natively.
    """
    ret = datetime.strptime(t[0:19],'%Y-%m-%d %H:%M:%S')
    if t[19]=='+':
        ret-=timedelta(hours=int(t[20:22]),minutes=int(t[23:]))
    elif t[19]=='-':
        ret+=timedelta(hours=int(t[20:22]),minutes=int(t[23:]))
    return ret