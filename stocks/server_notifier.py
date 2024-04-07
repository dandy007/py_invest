import requests
import platform
import time
from win10toast import ToastNotifier
 
def check_ip_port():
    try:
        url = f"http://192.168.2.168:8080"
        response = requests.get(url, timeout=5)
        return response.status_code
    except requests.exceptions.RequestException:
        return "Connection Failed"

def create_windows_notification(message):
    try:
        toaster = ToastNotifier()
        toaster.show_toast(
            "INVEST SERVER",
            message,
            duration=1
        )
    except (ImportError, OSError):
        print("Windows notification not supported.")

def notify_warn_mail():
    requests.get("https://hook.eu2.make.com/0vvp99q2utijgcd5epxbvnbew7geos7x", timeout=5)
    return

def main():

    status = check_ip_port()
    toast_msg = "STATUS: "
    if status == 200:
        toast_msg += "ONLINE"
    else:
        toast_msg += "!!! OFFLINE !!!"
        notify_warn_mail()

    create_windows_notification(toast_msg)
    return
    
if __name__ == "__main__":
    main()