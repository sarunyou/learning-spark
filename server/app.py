from flask import Flask
import requests

app = Flask('sample-app')

count_clicked = 0

@app.route("/")
def hello_world():
    global count_clicked
    count_clicked += 1
    return {
        "message": "hello",
        "clicked": count_clicked
    }


if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)
