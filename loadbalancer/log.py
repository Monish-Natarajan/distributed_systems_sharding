import datetime
def log(*args, **kwargs):
    current_time = datetime.now().strftime("[%Y-%m-%d %H:%M:%S] -- ")
    message = " ".join(map(str, args))
    print(current_time, message, **kwargs)
