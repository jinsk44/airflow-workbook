import logging
import os
import pathlib
import subprocess
from datetime import datetime

def on_success_callback(context):
    """
    Define the callback to post on Slack if a success is detected in the Workflow
    :return: operator.execute
    """
    operator = SlackAPIPostOperator(
        task_id='notify_success',
        text=str(context['task_instance']),
        token=Variable.get("slack_access_token"),
        channel=Variable.get("slack_channel")
    )
    return operator.execute(context=context)


def on_failure_callback(context):
     """
     Define the callback to post on Slack if a failure is detected in the Workflow
     :return: operator.execute
     """
     operator = SlackAPIPostOperator(
         task_id='notify_failure',
         text=str(context['task_instance']),
         token=Variable.get("slack_access_token"),
         channel=Variable.get("slack_channel")
     )

     return operator.execute(context=context)

if __name__ == '__main__':
    pass
