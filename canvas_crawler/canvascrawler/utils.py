import time
from functools import wraps
import tiktoken
import re
from urllib.parse import urlparse, unquote
from bs4 import BeautifulSoup

def backoff(max_retries=5, base_delay=1.0):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            delay = base_delay
            for attempt in range(1, max_retries+1):
                try:
                    return fn(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries:
                        raise
                    time.sleep(delay)
                    delay *= 2
            return None
        return wrapper
    return decorator

def count_tokens(text, model="gpt-3.5-turbo"):
    enc = tiktoken.encoding_for_model(model)
    return len(enc.encode(text))

def extract_hrefs(html_body: str) -> list[str]:
    """Return all href strings from an HTML fragment."""
    soup = BeautifulSoup(html_body or "", "html.parser")
    return [a["href"] for a in soup.find_all("a", href=True)]

_CANVAS_PATTERNS = [
    # pages: /courses/<cid>/pages/<slug>
    (re.compile(r"/courses/\d+/pages/([^/]+)"), "page", lambda m: unquote(m.group(1))),
    # assignments: /courses/<cid>/assignments/<aid>
    (re.compile(r"/courses/\d+/assignments/(\d+)"), "assignment", lambda m: int(m.group(1))),
    # discussion topics: /courses/<cid>/discussion_topics/(\d+)
    (re.compile(r"/courses/\d+/discussion_topics/(\d+)"), "discussion", lambda m: int(m.group(1))),
    # quizzes: /courses/<cid>/quizzes/(\d+)
    (re.compile(r"/courses/\d+/quizzes/(\d+)"), "quiz", lambda m: int(m.group(1))),
    # files: /files/(\d+)  (Canvas file URLs are often /files/<file_id>/...)
    (re.compile(r"/files/(\d+)"), "file", lambda m: int(m.group(1))),
    # files: /courses/<cid>/files/(\d+)
    (re.compile(r"/courses/\d+/files/(\d+)"), "file", lambda m: int(m.group(1))),
]

def classify_link(href: str, base_url: str) -> tuple[str,int] | None:
    """
    If href points to a Canvas resource we know how to queue, return (content_type, item_id).
    Otherwise return None.
    """
    # Strip off the domain if it's absolute
    parsed = urlparse(href)
    path = parsed.path
    # If this is a full Canvas URL, remove the base_url prefix
    if href.startswith(base_url):
        path = href[len(base_url):]

    for pattern, ctype, idfn in _CANVAS_PATTERNS:
        m = pattern.match(path)
        if m:
            return ctype, idfn(m)

    return None