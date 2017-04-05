import os
from flask import Flask, request, session, g, redirect, url_for, abort, render_template, flash
from werkzeug import secure_filename
import feedchecker

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads/'

@app.route('/')
def home():
	return render_template('home.html')

@app.route('/upload', methods=['GET', 'POST'])
def upload():
	if request.method == 'POST':
		feedfile = request.files['feedfile']
		if feedfile:
			filename = secure_filename(feedfile.filename)
			filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
			feedfile.save(filepath)
			data = { 'content': feedchecker.main(filepath, "|") }
			return render_template('show.html', **data)

if __name__ == "__main__":
	app.run()
