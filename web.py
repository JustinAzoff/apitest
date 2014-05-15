from bottle import Bottle, run

app = Bottle()

@app.route('/start')
def start():
    i = app.daemon.call("start")
    return {"id": i} 

@app.route('/status')
def status():
    s = app.daemon.sync_call("status")
    return s

@app.route('/time')
def time():
    return app.daemon.sync_call("time")

@app.route('/result/:id')
def result(id):
    id = int(id)
    return app.daemon.getresult(id)

@app.route('/log/:id')
def result(id):
    id = int(id)
    return {"log": app.daemon.getlog(id)}

def run_app(daemon):
    app.daemon = daemon
    run(app, host='localhost', port=8082)
