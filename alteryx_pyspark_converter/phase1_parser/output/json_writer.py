"""Write intermediate JSON representation to file or Databricks volume."""

import json
import os
import logging
from typing import Optional

from ..models.workflow import Workflow

logger = logging.getLogger(__name__)


class JSONWriter:
    """Write the parsed workflow intermediate representation to JSON."""

    DEFAULT_FILENAME = "intermediate.json"
    INDENT = 2

    def write(
        self,
        workflow: Workflow,
        output_path: str,
        filename: Optional[str] = None,
    ) -> str:
        """
        Write workflow intermediate JSON to the specified path.

        Args:
            workflow: Parsed Workflow object.
            output_path: Directory to write the JSON file.
            filename: Optional filename (default: intermediate.json).

        Returns:
            Full path to the written JSON file.
        """
        filename = filename or self.DEFAULT_FILENAME

        # Create output directory if it doesn't exist
        os.makedirs(output_path, exist_ok=True)

        file_path = os.path.join(output_path, filename)

        # Convert workflow to dict
        data = workflow.to_dict()

        # Write JSON
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=self.INDENT, ensure_ascii=False, default=str)

        logger.info("Wrote intermediate JSON to: %s", file_path)
        return file_path

    def write_string(self, workflow: Workflow) -> str:
        """
        Convert workflow to JSON string.

        Args:
            workflow: Parsed Workflow object.

        Returns:
            JSON string representation.
        """
        data = workflow.to_dict()
        return json.dumps(data, indent=self.INDENT, ensure_ascii=False, default=str)

    @staticmethod
    def load(json_path: str) -> Workflow:
        """
        Load a workflow from intermediate JSON.

        Args:
            json_path: Path to the intermediate JSON file.

        Returns:
            Workflow object.
        """
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        workflow = Workflow.from_dict(data)
        logger.info("Loaded intermediate JSON from: %s", json_path)
        return workflow

    @staticmethod
    def load_string(json_string: str) -> Workflow:
        """
        Load a workflow from a JSON string.

        Args:
            json_string: JSON string representation.

        Returns:
            Workflow object.
        """
        data = json.loads(json_string)
        return Workflow.from_dict(data)
