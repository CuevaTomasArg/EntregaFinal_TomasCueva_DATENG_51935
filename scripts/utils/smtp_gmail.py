import smtplib

def send_error(origin, password, to, title, text):
    try:
        smtp_conn = smtplib.SMTP('smtp.gmail.com', 587)
        smtp_conn.starttls()
        smtp_conn.login(origin, password)
        subject = title
        body_text = text
        message = 'Subject: {}\n\n{}'.format(subject, body_text)
        smtp_conn.sendmail(origin, to, message)
        print('Exito')
    except Exception as exception:
        print(exception)
        print('Failure')
        raise exception