import json, queue, sys, threading
from queue import Empty
from jupyter_client import KernelManager

def emit(kind, **payload): print(json.dumps({"kind": kind, **payload}, separators=(",", ":")), flush=True)
commands = queue.Queue()
def reader():
    for line in sys.stdin:
        try: commands.put(json.loads(line))
        except Exception as error: emit("bridgeError", message=str(error))
    commands.put({"type":"stop"})

def main():
    km=KernelManager(kernel_name="python3"); km.kernel_spec.argv[0]=sys.executable; km.start_kernel(cwd=sys.argv[1])
    client=km.blocking_client(); client.start_channels(); client.wait_for_ready(timeout=20)
    threading.Thread(target=reader, daemon=True).start(); emit("status", state="idle")
    try:
        while True:
            cmd=commands.get(); kind=cmd.get("type")
            if kind=="stop": break
            if kind=="interrupt": km.interrupt_kernel(); continue
            if kind!="execute": continue
            cell=cmd["cellId"]; mid=client.execute(cmd["code"], allow_stdin=True); emit("started", cellId=cell, requestId=mid)
            done=False
            while not done:
                try:
                    msg=client.get_iopub_msg(timeout=.05)
                    if msg.get("parent_header",{}).get("msg_id")!=mid: continue
                    typ,c=msg["msg_type"],msg["content"]
                    if typ=="status": emit("status",state=c["execution_state"])
                    elif typ=="stream": emit("stream",cellId=cell,name=c["name"],text=c["text"])
                    elif typ in ("execute_result","display_data"):
                        data=c.get("data",{}); emit("result",cellId=cell,executionCount=c.get("execution_count"),text=data.get("text/plain"),imagePng=data.get("image/png"))
                    elif typ=="error": emit("error",cellId=cell,name=c["ename"],value=c["evalue"],traceback=c.get("traceback",[]))
                except Empty: pass
                try:
                    req=client.get_stdin_msg(timeout=.001); c=req["content"]
                    emit("inputRequest",requestId=req["header"]["msg_id"],prompt=c["prompt"],password=c.get("password",False))
                    while True:
                        answer=commands.get()
                        if answer.get("type")=="inputReply": client.input(answer.get("value","")); break
                        if answer.get("type")=="interrupt": km.interrupt_kernel()
                except Empty: pass
                try:
                    reply=client.get_shell_msg(timeout=.001)
                    if reply.get("parent_header",{}).get("msg_id")==mid:
                        c=reply["content"]; emit("completed",cellId=cell,executionCount=c.get("execution_count"),status=c.get("status","unknown")); done=True
                except Empty: pass
    finally: client.stop_channels(); km.shutdown_kernel(now=True)
if __name__=="__main__": main()
