from gmail.gmail import gmail
import creds2


def send_gmail(subj, mail, to):
    email = gmail()
    email_id = creds2.email_id
    email_pw = creds2.email_pw
    email.login(email_id, email_pw)
    email.receiver_mail(to)
    email.send_mail(subj, mail)
    print("Mail sent successfully")

send_gmail("Test", "This is a test mail", "abc@gmail.com")