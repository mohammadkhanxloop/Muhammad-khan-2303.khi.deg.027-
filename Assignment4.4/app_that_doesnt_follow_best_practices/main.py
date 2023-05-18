import json
import logging
import threading
import time
import os
from flask import Flask, render_template, request

# debuging 
app = Flask(__name__)

# Solution for App should be able to be executed both during development, with debugging enabled, and in production, with debugging disabled.
app.debug = os.environ.get('DEBUG') == '1'


logging.basicConfig(
    filename="logs.txt",
    filemode="a",
    format="%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)



# Solution for App should be able to be executed both during development, with debugging enabled, and in production, with debugging disabled.
def check_debug():
    if(app.debug):
        return "/app/data/todo_deploy.json"
    else:
        return "/app/data/todo_prod.json"
TODO_FILE_NAME = check_debug()

if os.path.exists(TODO_FILE_NAME):
    with open(TODO_FILE_NAME) as f:
        TODO_ITEMS = json.loads(f)
else:
    TODO_ITEMS = []


def save_todo_items():
    with open(TODO_FILE_NAME, "w") as f:
        json.dumps(TODO_ITEMS, f)


saving_thread = threading.Thread(target=save_todo_items)
saving_thread.start()


@app.route("/", methods=["GET", "POST"])
def main():
    if request.method == "POST":
        content = request.form["content"]
        TODO_ITEMS.append(content)
    return render_template("index.html", todo_items=TODO_ITEMS)


if __name__ == "__main__":
    app.run(host="0.0.0.0")
