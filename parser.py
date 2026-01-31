import logging
from typing import List, Dict, Callable, Optional
import json
import os
import pandas as pd
from constant import Constant


logger = logging.getLogger(__name__)


class ClusterDataParser:
    ROLL = "roll"
    COMMUNICATION_GROUP_DOMAIN = 'communication_group'

    def __init__(self, params) -> None:
        self.events_summary: Optional[pd.DataFrame] = None
        self._profiler_type = params.get(Constant.PROFILER_TYPE, "mstx")
        self._data_type = params.get(Constant.DATA_TYPE, {})
        self._data_map = params.get(Constant.DATA_MAP, {})
        rank_list = params.get(Constant.RANK_LIST, 'all')
        self._rank_list = rank_list if rank_list == "all" else [int(rank) for rank in rank_list.split(",") if rank.isdigit()]
        pass

    def parse(self):
        """Parse data based on _profiler_type."""
        if self._profiler_type == "mstx":
            return self.cluster_parser_mstx()
        elif self._profiler_type == "nvtx":
            return self.cluster_parser_nvtx()
        else:
            raise ValueError(f"Unsupported profiler type: {self._profiler_type}")

    def cluster_parser_mstx(self):
        mapper_res = self.mapper_func()
        self.reducer_func(mapper_res)
        logger.info("Parsed in mstx")
        pass

    def cluster_parser_nvtx(self):
        logger.info("Parsed in mstx")
        pass

    def parse_rl_mstx_event(self, profiler_data_path: str, rank_id: int, roll: str) -> pd.DataFrame:
        """Parse MSTX json and return rows whose args contain event_type and domain as a DataFrame.

        Args:
            profiler_data_path: Path to the MSTX json file.
            rank_id: Rank id to attach to each row.
            roll: Role string to attach to each row.
        """
        data: List[Dict] = []
        events: List[Dict] = []

        # TODO: check file size
        with open(profiler_data_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        if data is None or not data:
            logger.warning(f"Rank {rank_id}: No MSTX events found in json")
            return events

        for row in data:
            args = row.get("args")
            if not isinstance(args, dict):
                continue
            if "event_type" not in args or "domain" not in args:
                continue
            # Convert nanoseconds to milliseconds
            ns_to_ms = Constant.NS_TO_US * Constant.US_TO_MS
            start_time_ms = row["ts"] / ns_to_ms
            end_time_ms = start_time_ms + row["dur"] / ns_to_ms
            duration_ms = row["dur"] / ns_to_ms

            event_data = {
                'name': row["name"],
                "roll": roll,
                'domain': row["domain"],
                'start_time_ms': start_time_ms,
                'end_time_ms': end_time_ms,
                'duration_ms': duration_ms,
                'rank_id': rank_id,
                'tid': row["tid"]
            }

            events.append(event_data)
        # 按照 start_time_ms 从小到大排序
        events.sort(key=lambda x: x['start_time_ms'])

        return events
    
    def mapper_func(self):
        data_maps = self._get_rank_path_with_roll()

        # Fall back to serial processing
        logger.info("Using serial processing for mapper_func")
        return [self._mapper_func(data_map) for data_map in data_maps]

    def _mapper_func(self, data_map):
        """Collect RL performance data from a single rank"""
        profiler_data_path = data_map.get(Constant.PROFILER_DATA_PATH)
        rank_id = data_map.get(self.RANK_ID)
        roll = data_map.get(self.ROLL)

        if not profiler_data_path:
            logger.warning(f"Rank {rank_id}: profiler_data_path not found")
            return None

        return self.parse_rl_mstx_event(profiler_data_path, rank_id, roll)

    def reducer_func(self, mapper_res):
        """Process data collected from all ranks"""
        # Remove None results
        reduce_results = [result for result in mapper_res if result is not None]

        if not reduce_results:
            logger.warning("No valid data collected from any rank")
            return

        self.events_summary = [
            event
            for events in reduce_results
            for event in events
        ]

        roll_rank_to_comm_groups = {}
        for event in self.events_summary:
            if event['domain'] == self.COMMUNICATION_GROUP_DOMAIN:
                roll_rank_to_comm_groups.setdefault((event['roll'], event['rank_id']), set()).add(event["name"])

        for event in self.events_summary:
            groups_set = roll_rank_to_comm_groups.get((event['roll'], event['rank_id']), set())
            event["communication_group"] = ",".join(groups_set) if groups_set else ""

        self.events_summary = pd.DataFrame(self.events_summary)

    def _get_profiler_data_path(self, rank_id, data_path):
        if self._data_type == Constant.TEXT:
            return os.path.join(data_path, Constant.SINGLE_OUTPUT, f"trace_view.json")  
        elif self._data_type == Constant.DB:
            return os.path.join(data_path, Constant.SINGLE_OUTPUT, f"ascend_pytorch_profiler_{rank_id}.db")
        else:
            raise ValueError(
                f"Unsupported data type: {self._data_type}. Supported type are: ['text', 'db']"
            )

    def _get_rank_path_with_roll(self) -> List[Dict]:
        """Get json path information for all ranks.

        This function is intentionally decoupled from class state; pass required
        dependencies in via arguments.
        """

        # TODO: support fixable rank list
        if self._rank_list != "all":
            logger.error("RL analysis currently only supports processing all ranks")
            return []

        rank_ids_with_roll = list(self._data_map.keys())
        data_paths: List[Dict] = []
        for task_roll, rank_id in rank_ids_with_roll:
            rank_path = self._data_map[(task_roll, rank_id)]
            profiler_data_path = self._get_profiler_data_path(rank_id, rank_path)

            data_path_dict = {
                Constant.RANK_ID: rank_id,
                self.ROLL: task_roll,
                Constant.PROFILER_DATA_PATH: "",
            }

            if os.path.exists(profiler_data_path):
                data_path_dict[Constant.PROFILER_DATA_PATH] = profiler_data_path
                data_paths.append(data_path_dict)
            else:
                logger.warning(
                    f"Profiler data file not found, rank id: {rank_id}, data path: {profiler_data_path}."
                )

        return data_paths

    def clean_data(self):
        self.events_summary = None

    def get_data(self):
        return self.events_summary

