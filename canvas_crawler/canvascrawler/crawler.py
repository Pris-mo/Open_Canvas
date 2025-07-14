from collections import deque
from .handlers import HandlerFactory

class CanvasCrawler:
    def __init__(self, client, course_id, storage, depth_limit, logger):
        self.client      = client
        self.course_id   = course_id
        self.storage     = storage
        self.depth_limit = depth_limit
        self.logger      = logger


    def _seed(self):
         # each context dict carries course_id, item_id, and current depth
        return [
#            ("syllabus",          {"course_id": self.course_id, "item_id": None, "depth": 0}),
            ("modules",           {"course_id": self.course_id, "item_id": None, "depth": 0}),
#            ("announcements",     {"course_id": self.course_id, "item_id": None, "depth": 0}),
            ("assignments", {"course_id": self.course_id, "item_id": None, "depth": 0}),
        ]

    def run(self):
        # seed with syllabus, modules, announcements, etc.
        queue = deque(self._seed())
        seen = set()

        while queue:
            content_type, context = queue.popleft()
            if context["depth"] > self.depth_limit:
                continue

            key = (content_type, context["item_id"])
            if key in seen:
                continue
            seen.add(key)

            try:
                handler = HandlerFactory.get_handler(content_type, self.client, self.storage, self.logger)
                handler.run(context)
            except Exception as e:
                self.logger.error(f"Failed to handle {content_type}/{context['item_id']}: {e}")
                continue

            # discover new links via parsed JSON or client calls...
            for link_type, new_context in self.discover_links(content_type, context):
                queue.append((link_type, new_context))

    def discover_links(self, content_type, context):
        # TODO: inspect storage or client to find linked Canvas items
        return []
