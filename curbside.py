"""
Further code solves the crawler code challenge published on the Curbside web site (http://challenge.curbside.com/)

Some key things to keep in mind:
- before you start crawling you have to obtain a session token
- session has expiration time, so if you make crawling in synchronous sequential way
  session will expire before you fetch results from all URLs, and you will not solve this challenge
- Curbside applies requests rate-limit, so you can't start too many crawlers simultaneously
"""

from collections import defaultdict
import json
import multiprocessing
import time

import requests

NO_SECRET_VALUE = "NO_SECRET"


class FetchTask:
    """
    FetchTask represents a fetching task for crawler
    """

    def __init__(self, url_id: str, parent_url_id: str, order_index: int) -> None:
        """
        :param url_id: URL id to fetch information from
        :param parent_url_id: id of the parent URL
        :param order_index: parent URL may have multiple child URLs, order_index keeps their order
        """
        self.url_id = url_id
        self.parent_url_id = parent_url_id
        self.order_index = order_index


class FetchedResult:
    """
    FetchedResult represents a result of a fetching task execution
    """

    def __init__(self, url_id: str, parent_url_id: str, value: str, order_index: int) -> None:
        """
        :param url_id: the URL id information has been fetched from
        :param parent_url_id: id of the parent URL
        :param value: keeps secret value from the fetched response if any
        :param order_index: parent URL may have multiple child URLs, order_index keeps their order
        """
        self.url_id = url_id
        self.parent_url_id = parent_url_id
        self.value = value
        self.order_index = order_index


class CSCrawler(multiprocessing.Process):
    """
    Curbside Crawler class
    """
    def __init__(self, base_url: str, session_token: str, task_queue: multiprocessing.JoinableQueue,
                 result_queue: multiprocessing.Queue, err_event: multiprocessing.Event) -> None:
        """
        :param base_url: base URL used during crawling
        :param session_token: session token value used with the HTTP "Session" header
        :param task_queue: queue from which crawler receives FetchTasks
        :param result_queue: queue on which crawler puts fetching results
        :param err_event: Event instance used as a flag when error occurs (other processes check this event)
        """
        super().__init__()
        self.id_url = base_url
        self.requests_session = self._get_requests_session(session_token)
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.err_event = err_event

    @staticmethod
    def _get_requests_session(session_token: str) -> requests.Session:
        """
        Creates and sets up requests session with the HTTP headers
        :param session_token: session token string value
        :return: session instance
        """
        s = requests.Session()
        s.headers.update({"Accept": "application/json", "Session": session_token})
        return s

    def run(self) -> None:
        """
        Main task of the crawler that fetches tasks from the queue and processes them
        :return: None
        """

        while True:
            fetch_task = self.task_queue.get()

            # if we need to stop crawler, we send None into the task queue
            # in case of error event we stop crawler as well
            if fetch_task is None or self.err_event.is_set():
                self.task_queue.task_done()
                break

            response = self.requests_session.get(self.id_url + fetch_task.url_id)

            if response.status_code != 200:
                self.task_queue.task_done()
                # set err_event so main process and other crawlers will stop their work
                self.err_event.set()

                print("Unable to fetch information from the {} URL id (response status code: {}, text: {})".format(
                    fetch_task.url_id, response.status_code, response.text))
                break

            # Curbside tries to make fetching a little bit challenging by mixing letter cases in the response
            # we force response to be in the lower case
            resp_json = json.loads(response.text.lower())

            # if there are any child URLs in the response we have to fetch
            if "next" in resp_json:
                # handling inconsistent type of the response key's value (may be either string or list, we force list):
                if isinstance(resp_json["next"], str):
                    resp_json["next"] = [resp_json["next"], ]

                # keeping the order in which URLs were fetched helps us
                # to restore the right order of letters in the message
                for ind, url_id in enumerate(resp_json["next"]):
                    self.task_queue.put(FetchTask(url_id, fetch_task.url_id, ind))

            self.result_queue.put(
                FetchedResult(fetch_task.url_id, fetch_task.parent_url_id, resp_json.get("secret", NO_SECRET_VALUE),
                              fetch_task.order_index)
            )

            self.task_queue.task_done()


class Curbside:
    """
    Starts crawlers, manages their lifecycle and processes result of their work
    """
    def __init__(self, session_url: str):
        """
        :param session_url: URL to fetch a session token from
        """
        self.root_index = "ROOT"
        self.session_token = self._get_session_token(session_url)
        self.task_q = multiprocessing.JoinableQueue()
        self.result_q = multiprocessing.Queue()
        self.result_tree = defaultdict(list)
        self.secret_message = []
        self.complete_event = multiprocessing.Event()
        self.err_event = multiprocessing.Event()

    @staticmethod
    def _get_session_token(session_url: str) -> str:
        """
        Fetches a session token from the Curbside
        :param session_url: URL to fetch session token from
        :return: session token
        """
        response = requests.get(session_url)
        if response.status_code != 200:
            raise Exception("Unable to obtain a session token")

        resp_body = response.json()
        if "session" not in resp_body:
            raise Exception("Response does not contain session token")
        return resp_body["session"]

    def _join_task_q(self) -> None:
        """
        Runs separate process that joins the task queue and sets the complete event.
        :return: None
        """

        def f(q, e):
            q.join()
            e.set()

        p = multiprocessing.Process(target=f, args=(self.task_q, self.complete_event))
        p.start()

    def get_crawler(self, base_url: str) -> CSCrawler:
        """
        Returns a single CSCrawler instance. All crawlers share the same task_q and results_q.
        :param base_url: base URL used when crawling
        :return: CSCrawler instance
        """

        return CSCrawler(base_url, self.session_token, self.task_q, self.result_q, self.err_event)

    def start_crawlers(self, base_url: str, crawler_count: int) -> None:
        """
        Starts requested amount of crawlers in sub-processes and blocks till task queue is empty (no URLs to fetch)
        :param base_url: base URL used when during the crawling
        :param crawler_count: amount of crawlers to start
        :return: None
        """
        if crawler_count <= 0:
            raise Exception("Positive count of crawlers expected ({} received)".format(crawler_count))

        for _ in range(crawler_count):
            c = self.get_crawler(base_url)
            c.start()

        # "start" is the first URL ID that must be fetched in this challenge
        self.task_q.put(FetchTask("start", self.root_index, 0))

        # join task queue in the separate process
        # that allows in the main process watch for 2 events (complete and error) at the same time
        # without blocking on joining the queue
        self._join_task_q()

        # watch for one of 2 events: complete (when separate process joins task queue) or error (when any crawler errors)
        while not self.complete_event.is_set() and not self.err_event.is_set():
            time.sleep(3)

        # on error event terminate all active children processes and terminate current process by raising exception
        if self.err_event.is_set():
            for p in multiprocessing.active_children():
                p.terminate()
            raise Exception("Error event fired by crawler")

        # after all URL IDs are fetched we need to stop all crawlers
        for _ in range(crawler_count):
            self.task_q.put(None)
        self.task_q.join()

    def _discover_secret_parts(self, url_id: str) -> None:
        """
        Reads the results_tree using pre-order traversal and appends parts of the secret message (if any)
        to the secret_message list

        :param url_id: URL ID result was fetched from
        :return: None
        """
        if self.result_tree[url_id]:
            # in order to get right order of letters we sort key entries based on their order_index
            self.result_tree[url_id].sort(key=lambda r: r.order_index)
            for result in self.result_tree[url_id]:
                if result.value != NO_SECRET_VALUE:
                    self.secret_message.append(result.value)

                self._discover_secret_parts(result.url_id)

    def get_secret_message(self) -> str:
        """
        Forms the secret message based on the items in the result queue
        :return: secret message string
        """

        # read all results from the queue, put them into the tree
        while not cs.result_q.empty():
            result = cs.result_q.get()
            self.result_tree[result.parent_url_id].append(result)

        # read the result tree and discover secret message
        self._discover_secret_parts(self.root_index)

        return "".join(self.secret_message)


if __name__ == "__main__":
    crawler_cnt = 16
    cs = Curbside("http://challenge.curbside.com/get-session")

    print("Starting {} crawlers... Please be patient, it may take some time to fetch all secrets.".format(crawler_cnt))
    cs.start_crawlers("http://challenge.curbside.com/", crawler_cnt)

    print("All secrets are fetched, processing results...")
    secret_message = cs.get_secret_message()
    print("\nSecret message: {}".format(secret_message))

    print("\nNow you know all secrets.")
