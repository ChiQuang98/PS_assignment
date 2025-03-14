from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import logging

# Initialize logger
logger = logging.getLogger(__name__)

class SlackAlert:
    def __init__(self, slack_token: str, channel_id: str):
        """
        Initialize the SlackAlert class with a Slack token and channel ID.
        """
        self.client = WebClient(token=slack_token)
        self.channel_id = channel_id

    def send_message(self, message: str, blocks=None):
        """
        Sends a message to the specified Slack channel.
        Args:
            message: The message text
            blocks: Optional Slack blocks for formatted messages
        """
        try:
            kwargs = {
                "channel": self.channel_id,
                "text": message,
            }
            if blocks:
                kwargs["blocks"] = blocks

            _ = self.client.chat_postMessage(**kwargs)
            logger.info(f"Message sent: {message}")
        except SlackApiError as e:
            error_msg = e.response.get("error", "Unknown error")
            logger.error(f"Got an error: {error_msg}")

    def alert_failure(
            self,
            task_name: str,
            dag_id: str = None,
            execution_date: str = None,
            error_message: str = None,
            log_output: str = None,
            ref_link: str = None
        ):
            """
            Sends a formatted failure alert message to the Slack channel.
            Args:
                task_name: Name of the failed task
                dag_id: ID of the DAG
                execution_date: Execution timestamp
                error_message: Error details if available
                log_output: Full log output from Airflow
                ref_link: Reference link for the failure
            """
            blocks = [
                {
                    "type": "header",
                    "text": {"type": "plain_text", "text": "🚨 Airflow Task Failure Alert"},
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Task:*\n{task_name}"},
                        {"type": "mrkdwn", "text": f"*DAG:*\n{dag_id or 'N/A'}"},
                        {
                            "type": "mrkdwn",
                            "text": f"*Execution Date:*\n{execution_date or 'N/A'}",
                        },
                    ],
                },
            ]

            if error_message:
                blocks.append(
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"*Error Message:*\n```{error_message}```",
                        },
                    }
                )

            if log_output:
                blocks.append(
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"*Full Log Output:*\n```{log_output}```",
                        },
                    }
                )

            if ref_link:
                blocks.append(
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"*Log URL Information:*\n```{ref_link}```",
                        },
                    }
                )

            message = f"Task '{task_name}' in DAG '{dag_id}' failed. Please check it ASAP."
            self.send_message(message, blocks=blocks)


