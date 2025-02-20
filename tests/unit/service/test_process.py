from multiprocessing import Queue

from dispatcher.service.process import ProcessManager, ProcessProxy


def test_pass_messages_to_worker():
    def work_loop(a, b, c, in_q, out_q):
        has_read = in_q.get()
        out_q.put(f'done {a} {b} {c} {has_read}')

    finished_q = Queue()
    process = ProcessProxy((1, 2, 3), finished_q, target=work_loop)
    process.start()

    process.message_queue.put('start')
    msg = finished_q.get()
    assert msg == 'done 1 2 3 start'


def test_pass_messages_via_process_manager():
    def work_loop(var, in_q, out_q):
        has_read = in_q.get()
        out_q.put(f'done {var} {has_read}')

    process_manager = ProcessManager()
    process = process_manager.create_process(('value',), target=work_loop)
    process.start()

    process.message_queue.put('msg1')
    msg = process_manager.finished_queue.get()
    assert msg == 'done value msg1'
