from abc import ABC, abstractmethod

class ContentHandler(ABC):
    def __init__(self, client, storage, logger):
        self.client  = client
        self.storage = storage
        self.logger  = logger

    def run(self, context):
        data   = self.fetch(context)             # 1) API call
        parsed = self.parse(context, data)       # 2) normalize/flatten into your JSON schema
        self.save(parsed)                        # 3) write JSON + download raw files

    @abstractmethod
    def fetch(self, context):
        ...

    @abstractmethod
    def parse(self, data):
        ...

    def save(self, parsed):
        # default implementation, or override in subclasses
        self.storage.write_json(parsed)
        if parsed.get("file_url"):
            self.storage.download_file(parsed["file_url"], parsed["file_path"])

class PageHandler(ContentHandler):
    def fetch(self, context):
        return self.client.get_wiki_page(context.course_id, context.item_id)

    def parse(self, data):
        return {
          "id":       data["id"],
          "title":    data["title"],
          "type":     "page",
          "url":      data["html_url"],
          # …etc…
        }

class AssignmentHandler(ContentHandler):
    def fetch(self, context):
        temp = ''
        return self.client.get_assignment(context["course_id"], context["item_id"])

    def parse(self, context, data):
        temp = ''
        return {
            "id":       data["id"],
            "title":    data["name"],
            "type":     "assignment",
            "due_at":   data.get("due_at"),
            "points_possible": data.get("points_possible"),
            "depth":    context["depth"],
            "url":      data["html_url"],
        }

class SyllabusHandler(ContentHandler):
    def fetch(self, context):
        course_data = self.client.get_course(context["course_id"],True)
        return course_data
    def parse(self, context, data):
        return {
            "id":    data["id"],
            "type":  "syllabus",
            "title": data.get("name"),
            "data":  data.get("syllabus_body"),    
            "depth": context["depth"]
        }
    
class ModulesHandler(ContentHandler):
    def fetch(self, context):
        modules = self.client.get_modules(context["course_id"])
        return modules
    def parse(self, context, data):
        return {
            "type":    "modules",
            "course":  context["course_id"],
            "items":   [m["id"] for m in data],
            "depth":   context["depth"]
        }
        

class AnnouncementsHandler(ContentHandler):
    def fetch(self, context):
        return self.client.get_announcements(context["course_id"])
    def parse(self, context, data):
        return {
            "type":    "announcements",
            "course":  context["course_id"],
            "items":   [a["id"] for a in data],
            "depth":   context["depth"]
        }
    
class AssignmentsHandler(ContentHandler):
    def fetch(self, context):
        return self.client.get_assignments(context["course_id"])
    def parse(self, context, data):
        return {
            "type":     "assignments",
            "course":   context["course_id"],
            "assignments":   [a["id"] for a in data],
            "depth":    context["depth"]
        }


class PageHandler(ContentHandler):
    def fetch(self, context):
        return self.client.get_wiki_page(context["course_id"], context["item_id"])

    def parse(self, context, data):
        return {
            "id":       data["page_id"],
            "title":    data["title"],
            "type":     "page",
            "url":      data["html_url"],
            "depth":    context["depth"]
        }

class DiscussionHandler(ContentHandler):
    def fetch(self, context):
        return self.client.get_discussion_topic(context["course_id"], context["item_id"])

    def parse(self, context, data):
        return {
            "id":       data["id"],
            "title":    data["title"],
            "type":     "discussion",
            "url":      data["html_url"],
            "depth":    context["depth"]
        }

class ModuleHandler(ContentHandler):
    def fetch(self, context):
        module_data = self.client.get_module(context["course_id"], context["item_id"])
        module_items = self.client.get_module_items(context["course_id"], context["item_id"])
        module_data["items"] = module_items  
        return module_data

    def parse(self, context, data):
        module_data = {
            "type":   "module",
            "id":     data["id"],
            "title":  data["name"],
            "items":  [i["id"] for i in data["items"]], 
            "depth":  context["depth"],
        }
        return module_data


# And finally the factory:
class HandlerFactory:
    registry = {
        "syllabus":          SyllabusHandler,
        "modules":           ModulesHandler,
        "announcements":     AnnouncementsHandler,
        "assignments":       AssignmentsHandler,
        "module":            ModuleHandler,
        "page":              PageHandler,
        "discussion":        DiscussionHandler,
        "assignment":        AssignmentHandler,
        # files, external links,
    }

    @classmethod
    def get_handler(cls, content_type, client, storage, logger):
        Handler = cls.registry.get(content_type)
        if not Handler:
            raise ValueError(f"No handler for {content_type}")
        return Handler(client, storage, logger)