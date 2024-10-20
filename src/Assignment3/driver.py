from utill import process_user_activity

# Driver code
if __name__ == "__main__":
    # Call the function to process user activity
    past_7days = process_user_activity()

    # Show past 7 days activity
    past_7days.show(truncate=False)