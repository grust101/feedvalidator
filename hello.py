from flask import Flask, request, session, g, redirect, url_for, abort, render_template, flash
import feedchecker

app = Flask(__name__)


@app.route("/")
def home():
	return render_template('home.html')

@app.route("/check")
def show():
	filepath = request.args.get("file")
	data = {
		'content': feedchecker.main(filepath, "|")
	}
	return render_template('show.html', **data)

if __name__ == "__main__":
	app.run()