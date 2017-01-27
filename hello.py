from flask import Flask, request, session, g, redirect, url_for, abort, render_template, flash
import feedchecker

app = Flask(__name__)

@app.route("/")
def hello():
	content = feedchecker.main("catalog_full_republicoftea_01_24_2017.zip", "|")
	return render_template('show.html', content=content)

if __name__ == "__main__":
	app.run()