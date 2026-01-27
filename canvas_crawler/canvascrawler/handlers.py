from abc import ABC, abstractmethod

class ContentHandler(ABC):
    def __init__(self, client, storage, logger):
        self.client  = client
        self.storage = storage
        self.logger  = logger

    def run(self, context):
        data   = self.fetch(context)             # 1) API call
        
        # if the content is locked, handle accordingly
        if isinstance(data, dict) and data.get("locked_for_user") is True:
            parsed = self.parse_locked(context, data)
            self.save(parsed)  # save stub JSON; body is empty so no HTML written
            self.logger.warning(
                f"Locked content: type={context.get('content_type')} id={parsed.get('id')} "
                f"course={context.get('course_id')} item={context.get('item_id')} "
                f"reason={parsed.get('lock_explanation')!r}"
            )
            return parsed
        
        # normal processing
        parsed = self.parse(context, data)       # 2) normalize/flatten into your JSON schema
        self.save(parsed)                        # 3) write JSON + download raw files
        return parsed
    

    @abstractmethod
    def fetch(self, context):
        ...

    @abstractmethod
    def parse(self, context, data):
        ...

    def parse_locked(self, context, data):
        """
        Default locked-content stub.
        Subclasses can override if they can extract better title/url fields.
        """
        return {
            "type": context.get("content_type", "unknown"),  # optional; see note below
            "id": data.get("id", context.get("item_id")),
            "title": data.get("title") or data.get("name"),
            "url": data.get("html_url") or data.get("url"),
            "depth": context.get("depth"),
            "locked_for_user": True,
            "lock_explanation": data.get("lock_explanation") or data.get("unlock_at"),
            "body": "",
            "file_path": f"locked/{data.get('id', context.get('item_id'))}.html",
        }

    def save(self, parsed):
        # default implementation, or override in subclasses
        self.storage.write_json(parsed)
        if parsed.get("body"):
            self.storage.write_html(parsed["body"], parsed["file_path"])

class AssignmentHandler(ContentHandler):
    def run(self, context):
        data = self.fetch(context)

        # locked handling still applies
        if isinstance(data, dict) and data.get("locked_for_user") is True:
            parsed = self.parse_locked(context, data)
            self.save(parsed)
            self.logger.warning(
                f"Locked content: type={context.get('content_type')} id={parsed.get('id')} "
                f"course={context.get('course_id')} item={context.get('item_id')} "
                f"reason={parsed.get('lock_explanation')!r}"
            )
            return parsed

        # SAFETY NET: New Quizzes appear as assignments but must be handled via modules
        if isinstance(data, dict) and data.get("quiz_lti") is True:
            self.logger.info(
                f"Skipping New Quiz discovered via assignments: "
                f"course={context.get('course_id')} assignment_id={data.get('id')} "
                f"(should be discovered via modules instead)"
            )
            return None  # important: tell crawler nothing was parsed

        # normal assignment path
        parsed = self.parse(context, data)
        self.save(parsed)
        return parsed

    def fetch(self, context):
        return self.client.get_assignment(context["course_id"], context["item_id"])

    def parse(self, context, data):
        return {
            "id":       data["id"],
            "title":    data["name"],
            "type":     "assignment",
            "due_at":   data.get("due_at"),
            "points_possible": data.get("points_possible"),
            "depth":    context["depth"],
            "url":      data["html_url"],
            "body":   data.get("description", ""),
            "file_path": f"assignments/{data['id']}.html",
        }

class NewQuizHandler(ContentHandler):
    def fetch(self, context):
        tmp = self.client.get_new_quiz(context["course_id"], context["item_id"])
        return tmp

    def parse(self, context, data):
        url = f'{self.client.server_url}/courses/{context["course_id"]}/quizzes/{data["id"]}'
        allowed_attempts = (
            data.get("quiz_settings", {})
                .get("multiple_attempts", {})
                .get("max_attempts", "")
        )
        return {
            "id":       data["id"],
            "title":    data["title"],
            "type":     "new_quiz",
            "due_at":   data.get("due_at"),
            "points_possible": data.get("points_possible"),
            "allowed_attempts": allowed_attempts,         
            "scoring_policy": data['grading_type'],                
            "time_limit": data['quiz_settings']['session_time_limit_in_seconds'],
            "depth":    context["depth"],
            "url":     url,
            "body":   data.get("instructions", ""),
            "file_path": f"new_quizzes/{data['id']}.html",
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
            "depth": context["depth"],
            "body": data.get("syllabus_body", ""),
            "file_path": f"syllabus/{data['id']}.html",
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
        tmp = ''
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
            "depth":    context["depth"],
            "body":     data["body"],
            "file_path": f"pages/{data['page_id']}.html",
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
            "depth":    context["depth"],
            "body":     data.get("message", ""),
            "file_path": f"discussions/{data['id']}.html",      
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
    

class FileHandler(ContentHandler):
    def fetch(self, context):
        return self.client.get_file(context["course_id"], context["item_id"])

    def parse(self, context, data):
        tmp = ''
        file_data = {
            "id":       data["id"],
            "title":    data["display_name"],
            "type":     "file",
            "extension": data["filename"].split('.')[-1] if '.' in data["filename"] else data['content-type'].split('/')[-1],
            "url":      data["url"],
            "depth":    context["depth"]
        }
        file_data["file_path"] = f"files/{data['id']}.{file_data['extension']}"
        return file_data
    
    def save(self, parsed):
        self.storage.write_json(parsed)
        # Download the actual file
        if parsed.get("url"):
            self.storage.download_file(parsed["url"], parsed["file_path"])
        else:
            self.logger.warning(f"No URL for file {parsed['id']}, skipping download.")


class classicQuizHandler(ContentHandler):
    def fetch(self, context):
        return self.client.get_classic_quiz(context["course_id"], context["item_id"])

    def parse(self, context, data):
        return {
            "id":       data["id"],
            "title":    data["title"],
            "type":     data['quiz_type'],
            "due_at":   data.get("due_at"),
            "points_possible": data.get("points_possible"),
            "allowed_attempts": data.get("allowed_attempts"),
            "scoring_policy": data.get("scoring_policy"),
            "number_of_questions": data.get("question_count"),
            "time_limit": data.get("time_limit"),
            "depth":    context["depth"],
            "url":      data["html_url"],
            "body":   data.get("description", ""),
            "file_path": f"quizzes/{data['id']}.html",
        }

# The factory:
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
        "file":              FileHandler,
        "quiz":              classicQuizHandler,
        "new_quiz":          NewQuizHandler,
        # files, external links,
    }

    @classmethod
    def has_handler(cls, content_type: str) -> bool:
        return content_type in cls.registry
    
    @classmethod
    def get_handler(cls, content_type, client, storage, logger):
        Handler = cls.registry.get(content_type)
        if not Handler:
            raise ValueError(f"No handler for {content_type}")
        return Handler(client, storage, logger)
    