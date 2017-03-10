from __future__ import absolute_import, division, print_function

from flask_wtf import Form
from wtforms import BooleanField, PasswordField, StringField, SubmitField
from wtforms.fields import TextAreaField
from wtforms.validators import DataRequired

from ..core.mdx.parser.parse import MdxParser


class QueryForm(Form):
    """
    Query Form
    """
    mdx = TextAreaField(
        "MDX Query",
        validators=[DataRequired(message="Please enter the MDX Query")])

    def validate(self):
        """
        Valide
        :return:
        """

        parser = MdxParser()
        if self.mdx.data:
            try:
                parser.parsing_mdx_query('all', query=self.mdx.data)
                print(parser.parsing_mdx_query('all', query=self.mdx.data))
            except:
                self.mdx.errors = list(self.mdx.errors)
                self.mdx.errors.append('Invalide MDX Query !!')
                self.mdx.errors = tuple(self.mdx.errors)
                return False

        if not Form.validate(self):
            return False
        return True


class LoginForm(Form):
    """
    Loging Form
    """
    username = StringField(
        'Your username:',
        validators=[DataRequired(message="Please enter the Username")])
    password = PasswordField(
        'Password',
        validators=[DataRequired(message="Please enter the Password")])
    remember_me = BooleanField('Remember Me ')
    submit = SubmitField('Log In')
