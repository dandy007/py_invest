import requests
from icalendar import Calendar, Event
from datetime import datetime, timedelta

# Define the events
events = [
    {
        "summary": "Meeting with John",
        "start": datetime(2023, 4, 8, 10, 0, 0),
        "end": datetime(2023, 4, 8, 11, 0, 0)
    },
    {
        "summary": "Lunch with Jane",
        "start": datetime(2023, 4, 9, 12, 30, 0),
        "end": datetime(2023, 4, 9, 13, 30, 0)
    },
    {
        "summary": "Conference call",
        "start": datetime(2023, 4, 10, 14, 0, 0),
        "end": datetime(2023, 4, 10, 15, 0, 0)
    }
]

# Create the iCalendar object
cal = Calendar()
cal.add('prodid', '-//My calendar product//mxm.dk//')
cal.add('version', '2.0')

# Add the events to the iCalendar
for event in events:
    ical_event = Event()
    ical_event.add('summary', event['summary'])
    ical_event.add('dtstart', event['start'])
    ical_event.add('dtend', event['end'])
    cal.add_component(ical_event)

# Convert the iCalendar to a string
ical_str = cal.to_ical().decode('utf-8')

# Define the webhook URL
webhook_url = "https://example.com/webhook"

# Send the iCalendar file to the webhook
headers = {
    "Content-Type": "text/calendar"
}
response = requests.post(webhook_url, data=ical_str, headers=headers)

if response.status_code == 200:
    print("iCalendar file sent successfully!")
else:
    print(f"Failed to send iCalendar file. Status code: {response.status_code}")