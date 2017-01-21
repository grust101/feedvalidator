from flask import Flask, request, session, g, redirect, url_for, abort, render_template, flash
import feedchecker

app = Flask(__name__)

@app.route("/")
def hello():
	entries = feedchecker.main("../../../Desktop/catalog_full_guitarcenter_2017_01_21.zip", "|")
	return render_template('show.html', entries=entries)

if __name__ == "__main__":
	app.run()