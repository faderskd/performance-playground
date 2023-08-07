import logging
import os
import sys
from empty import Empty
from flask import Flask, render_template

from mixins import HttpMixin

# define base classes for our App class
base_cls_list = [Empty]
base_cls_list = [HttpMixin] + base_cls_list

# apps is a special folder where you can place your blueprints
PROJECT_PATH = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, os.path.join(PROJECT_PATH, "apps"))

basestring = getattr(__builtins__, 'basestring', str)

# dynamically create our class
App = type('App', tuple(base_cls_list), {})


def config_str_to_obj(cfg):
    if isinstance(cfg, basestring):
        module = __import__('config', fromlist=[cfg])
        return getattr(module, cfg)
    return cfg


def create_app(config_object="config"):
    """Create application factory, as explained here: http://flask.pocoo.org/docs/patterns/appfactories/.

    :param config_object: The configuration object to use.
    """
    app = Flask(__name__.split(".")[0])
    app.config.from_object(config_object)
    register_extensions(app)
    register_blueprints(app)
    register_errorhandlers(app)
    register_commands(app)
    configure_logger(app)
    return app


def register_extensions(app):
    pass


def register_blueprints(app):
    from apps.broker import app as broker

    app.register_blueprint(broker, url_prefix='/broker')


def register_errorhandlers(app):
    def render_error(error):
        """Render error template."""
        # If a HTTPException, pull the `code` attribute; default to 500
        error_code = getattr(error, "code", 500)
        return render_template(f"http/{error_code}.html"), error_code

    for errcode in [401, 404, 500]:
        app.errorhandler(errcode)(render_error)


def register_commands(app):
    from commands import new_app, test_cmd, print_profile

    app.cli.add_command(new_app)
    app.cli.add_command(test_cmd)
    app.cli.add_command(print_profile)


def configure_logger(app):
    handler = logging.StreamHandler(sys.stdout)
    if not app.logger.handlers:
        app.logger.addHandler(handler)


def app_factory(config, app_name):
    from commands import new_app, test_cmd
    from apps.broker import app as broker

    # you can use Empty directly if you wish
    app = App(app_name, template_folder=os.path.join(PROJECT_PATH, 'templates'))
    config = config_str_to_obj(config)

    app.cli.add_command(new_app)
    app.cli.add_command(test_cmd)

    app.register_blueprint(broker, url_prefix='/broker')
    app.setup()

    return app
