class User:

    def __init__(self, first_name=None, last_name=None, email=None, initials=None):
        self.first_name = first_name
        self.last_name = last_name
        self.email = email
        self.initials = initials

    @staticmethod
    def create_from_rest_resource(resource):
        user = User(first_name=resource.first_name, last_name=resource.last_name,
                    email=resource.email, initials=resource.initials)
        return user
