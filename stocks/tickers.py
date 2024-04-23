import os
import time
import sys

# Function to clear the console screen
def clear_screen():
    # Windows
    if os.name == 'nt':
        os.system('cls')
    # MacOS/Linux
    else:
        os.system('clear')

# Animation parameters
width = 30  # Width of the console window for the animation
bar_length = 10  # Length of the moving bar

while True:
    for i in range(width - bar_length + 1):
        # Clear the screen
        clear_screen()
        
        # Print the bar at position i
        print(' ' * i + '=' * bar_length)
        
        # Delay to control animation speed
        time.sleep(0.1)
        
    for i in range(width - bar_length, 0, -1):
        # Clear the screen
        clear_screen()
        
        # Print the bar at position i
        print(' ' * i + '=' * bar_length)
        
        # Delay to control animation speed
        time.sleep(0.1)