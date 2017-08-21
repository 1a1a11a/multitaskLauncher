""" the view functions for flask 


"""


import os
import sys
import time 
import argparse
from flask import Flask
from flask import render_template, request 


sys.path.append(os.getcwd())
from utilPrinting import * 
from utilNode import * 
from taskModule import load_task_status 



LAST_CHECK_TIME = 0 
NODES = {}          # nodeIP -> (ussername, mem, CPU) 
app = Flask(__name__)







@app.route("/")
@app.route("/index")
@app.route("/node_status", methods=['GET', 'POST']) 
def default():
    global NODES
    global LAST_CHECK_TIME 

    if not os.path.exists("nodes"):
        print("cannot find nodes file exit")
        # os.remove(str(os.getpid()) + ".monitoring.out") 

        func = request.environ.get('werkzeug.server.shutdown')
        func()


    if request.method == 'POST':
        print(request.form)
        if 'stop' in request.form:
            print("received stop") 
            # stop_workers()
        elif 'start' in request.form:
            print("received start") 
            # start_workers()
        elif 'restart' in request.form: 
            print("received restart") 
            # restart_workers() 
        elif 'refresh' in request.form: 
            pass 
        elif 'submit' in request.form:
            
            request.form['threads']

        else:
            print("received {}\n".format(request.form['submit'])) 

    if (time.time() - LAST_CHECK_TIME) > 5:
        LAST_CHECK_TIME = time.time() 
        if len(NODES) == 0: 
            INFO("load node info")
            load_nodes_file(NODES) 
            INFO("node info loaded, {} nodes".format(len(NODES)))

        get_node_health_mt(NODES, n_threads=16)
        # get_node_health(NODES)

        # read_monitor_log("../log/monitor.log", NODES, WORKER_STATUS_LEN)

    # node info
    node_info_list = []
    for node, nodeinfo in NODES.items():
        nh = nodeinfo[1]
        node_info_list.append(("{}@{}".format(nodeinfo[0], node), 
                (nh.n_threads, nh.loadavg, nh.memory_used, nh.memory_total, nh.disk, nh.network)))
        node_info_list.sort(key=lambda x:x[0])

    # task info 
    task_info_list = load_task_status(task_status_loc="task.status")

    return render_template("worker_status.html", workers=node_info_list, tasks=task_info_list,
        ptime=time.strftime("%B.%d %H:%M:%S", time.localtime(LAST_CHECK_TIME))) 



@app.route("/test")
def test():
    NODES = {}

    read_monitor_log("../log/monitor.log", NODES, WORKER_STATUS_LEN)

    info_list = sorted( zip( list(NODES.keys()), list(NODES.values()) ) )
    return render_template("worker_status.html", workers=info_list, 
        ptime=time.strftime("%B.%d %H:%M:%S", time.localtime())) 


def monitoring_run(listening_scope, listening_port):

    assert os.path.exists("nodes"), "nodes info does not exist" 
    sys.stdout = open(str(os.getpid()) + ".monitoring.out", "w") 
    sys.stderr = sys.stdout 

    if listening_scope == "public": 
        INFO("monitoring service start with public access")
        app.run(debug=False, host="0.0.0.0", port=listening_port) 
    elif listening_scope == "local": 
        INFO("monitoring service start with local access")
        app.run(debug=False, port=listening_port) 
    else:
        raise RuntimeError("unknown listening scope: {}".format(listening_scope))





if __name__ == "__main__": 
    assert os.path.exists("nodes"), "nodes info does not exist" 
    parser = argparse.ArgumentParser(description='multiTaskSubmitter monitoring module')


    # parser.add_argument('scope', metavar='N', type=int, nargs='+',
    # parser.add_argument("config", help="the config file that contains the node information")
    parser.add_argument("scope", help='the scope that monitoring service available to, "\
        "allowed value: public/local', default="public")
    parser.add_argument("port", type=int, help="the port that monitoring service listens on", default=5002)

    args = parser.parse_args()

    listening_scope = args.scope
    listening_port = args.port 
    if listening_scope == "public": 
        INFO("monitoring service start with public access")
        app.run(debug=True, host="0.0.0.0", port=listening_port) 
    elif listening_scope == "local": 
        INFO("monitoring service start with local access")
        app.run(debug=True, port=listening_port) 
    else:
        raise RuntimeError("unknown listening scope: {}".format(args.scope))

