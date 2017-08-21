

import os
import sys 
import string
import subprocess 
from concurrent.futures import ThreadPoolExecutor, as_completed 


sys.path.append(os.getcwd())
from utilPrinting import * 


class nodeHealth:
    def __init__(self, n_threads=-1, 
                       loadavg="not available", 
                       memory_total="not available", 
                       memory_used="not available", 
                       disk="not available", 
                       network="not available",
                       loadavg2="not available", 
                       loadavg3="not available"
                ):
        self.n_threads = n_threads
        self.memory_total = memory_total 
        self.memory_used = memory_used 
        self.disk = disk 
        self.network = network 
        self.loadavg = loadavg 

        if ' ' in loadavg and loadavg != "not available": 
            # example: 0.02 0.02 0.05 
            try:    
                self.loadavg, self.loadavg2, self.loadavg3 = loadavg.strip(string.whitespace).split()
            except Exception as e:
                WARNING("failed to split loadavg: {}, string: {}".format(e, loadavg))


    def __str__(self):
        ret_str = "memory: {}/{}, loadavg: {}/{}".format(self.memory_used, 
                    self.memory_total, self.loadavg, self.n_threads) 
        if self.disk != "not available":
            ret_str += ", disk: {}".format(self.disk) 
        if self.network != "not available": 
            ret_str += ", network: {}".format(self.network)
        return ret_str

    def __repr__(self): 
        return self.__str__() 

    def to_list(self):
        return [self.memory_used, self.memory_total, 
                self.loadavg, self.n_threads, self.disk, self.network] 



def check_node_health(username, node, check_type="normal", print_out=False):
    """check the health of node, retrieve memory and CPU information 
            
    Arguments:
        username {[string]} -- username for login
        node {[string]} -- domain name or ip address 
    
    Keyword Arguments:
        print_out {bool} -- whether print out node information (default: {False})
    """
    if check_type == "normal": 
        p = subprocess.run('''ssh -o StrictHostKeyChecking=no -o connectTimeout=2 {}@{} "
                            grep -c ^processor /proc/cpuinfo; 
                            cat /proc/loadavg | cut -d ' ' -f 1,2,3 ; 
                            free -m | grep Mem |tr -s ' '|cut -d ' ' -f 2,3; " '''.format(username, node), 
            shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output = p.stdout.decode("utf-8").strip(string.whitespace).split("\n")
        err = p.stderr.decode("utf-8").strip(string.whitespace)
        
        if len(err): 
            WARNING(err)
            return nodeHealth() 
            # return (-1, -1)
        else:
            n_threads = output[0]
            loadavg = output[1]
            memory_total = '{}'.format(output[2].split()[0])
            memory_used = '{}'.format(output[2].split()[1])
            nh = nodeHealth(n_threads=n_threads, loadavg=loadavg, 
                            memory_total=memory_total, memory_used=memory_used)
            if print_out:
                print("{} {}".format(node, nh))
            return nh 
    else:
        ERROR("other check_type not supported yet")


def get_node_health_mt(nodes_dict, check_type="normal", n_threads=8, print_out=False):
    """use multithreading to check each node health
        
    Arguments:
        nodes_dict {dict} -- [nodesIP(domainName)->(username, mem, CPU)]
    
    Keyword Arguments:
        check_type {str} -- [description] (default: {"normal"})
        n_threads {number} -- [description] (default: {8})
    """ 

    with ThreadPoolExecutor(max_workers=n_threads) as executor:
        futures = {executor.submit(check_node_health, nodeinfo[0], node, check_type, print_out): node 
                            for node, nodeinfo in nodes_dict.items()} 
        for future in as_completed(futures):
            node = futures[future]
            nodeinfo = nodes_dict[node]
            result = future.result()
            nodes_dict[node] = (nodeinfo[0], result)
            
            # print("{} {}".format(node, nodes_dict[node]))



def get_node_health(nodes_dict, check_type="normal"):
    """check each node health and update nodes_dict 
        
    Arguments:
        nodes_dict {dict} -- [nodesIP(domainName)->(username, mem, CPU)]
    
    Keyword Arguments:
        check_type {str} -- [description] (default: {"normal"})
    """ 

    for node, nodeinfo in nodes_dict.items():
        nodes_dict[node] = (nodes_dict[node][0], check_node_health(nodeinfo[0], node))
        print("{} {}".format(node, nodes_dict[node]))


    print("finished threadpoolexecutor, nodes {}".format(nodes_dict))


def load_nodes_file(nodes_dict, file_loc="nodes"):
    """used only by monitoring service 
    the input file has format 
    user@nodeIP(or nodeDomainName)
        
    Keyword Arguments:
        file_loc {str} -- [description] (default: {"nodes"})
    """
    with open(file_loc) as ifile:
        for line in ifile:
            line_split = line.split("@")
            nodes_dict[line_split[1].strip(string.whitespace)] = (line_split[0], nodeHealth())

    return nodes_dict 

def exec_code_on_node(username, node, code):
    subprocess.run('''ssh -o StrictHostKeyChecking=no {}@{} "{}" '''.format(username, node, code), 
            shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)



def exec_code_on_nodes_mt(nodes_dict, code, n_threads=8):
    """execute arbitrary code on nodes, ignore errors 
        
    Arguments:
        nodes_dict {dict} -- [nodesIP(domainName)->(username, mem, CPU)]
    
    Keyword Arguments:
        n_threads {number} -- [description] (default: {8})
    """ 

    with ThreadPoolExecutor(max_workers=n_threads) as executor:
        futures = {executor.submit(exec_code_on_node, nodeinfo[0], node, code): node 
                            for node, nodeinfo in nodes_dict.items()} 
        for future in as_completed(futures):
            result = future.result()
