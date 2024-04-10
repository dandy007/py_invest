import requests
from icalendar import Calendar, Event
from datetime import datetime, timedelta
from data_providers.alpha_vantage import get_earnings_calendar

def export_ical_to_hook(ical):
    # Convert the iCalendar to a string
    ical_str = ical.to_ical().decode('utf-8')

    # Define the webhook URL
    webhook_url = "https://hook.eu2.make.com/c8lbbx3jknwsw77u32wew03yl8mg59yd"

    # Send the iCalendar file to the webhook
    headers = {
        "Content-Type": "text/calendar"
    }
    response = requests.post(webhook_url, data=ical_str, headers=headers)

    if response.status_code == 200:
        print("iCalendar file sent successfully!")
    else:
        print(f"Failed to send iCalendar file. Status code: {response.status_code}")
    return

def create_earnings_ical(earnings):
    # data.append([tickerId, name, earning_date, fiscal_date, estimate, currency])

    # Create the iCalendar object
    cal = Calendar()
    cal.add('prodid', '-//Earnings calendar//mxm.dk//')
    cal.add('version', '2.0')

    date_format = "%Y-%m-%d %H"

    # Add the events to the iCalendar
    for earning in earnings:

        date_obj = datetime.strptime(f"{earning[2]} 20", date_format)

        ical_event = Event()
        ical_event.add('summary', f"Earnings: {earning[0]} - {earning[1]}") # Ticker - Name
        ical_event.add('dtstart', date_obj) # Release date
        ical_event.add('dtend', date_obj) # Release date
        cal.add_component(ical_event)

    return cal

def export_earnings(tickers):
    earnings = get_earnings_calendar()

    filtered_earnings = []
    for earning in earnings:
        if earning[0] in tickers:
            filtered_earnings.append(earning)

    export_ical_to_hook(create_earnings_ical(filtered_earnings))


if __name__ == "__main__":
    export_earnings()