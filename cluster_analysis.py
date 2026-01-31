import argparse
from ast import arg
import os
from parser import ClusterDataParser, get_cluster_parser_fn
from visualizer import get_cluster_visualizer_fn
from omegaconf import DictConfig
from constant import Constant
from data_preprocessor import DataPreprocessor

# TODO: support more profile data e.g. MindsporeDataPreprocessor
def allocate_prof_data(input_path: str):
    """Allocate and process profiling data from input path."""
    ascend_pt_dirs = []
    for root, dirs, _ in os.walk(input_path):
        for dir_name in dirs:
            if dir_name.endswith(Constant.PT_PROF_SUFFIX):
                ascend_pt_dirs.append(os.path.join(root, dir_name))
    data_processor = DataPreprocessor(ascend_pt_dirs)
    data_map = data_processor.get_data_map()
    return data_map

def main():
    arg_parser = argparse.ArgumentParser(description="集群调度可视化")
    arg_parser.add_argument("--input-path", default="test", help="profiling数据的原始路径")
    arg_parser.add_argument("--profiler-type", default="mstx", help="性能数据种类")
    arg_parser.add_argument("--output-path", default="test", help="输出路径")
    arg_parser.add_argument("--vis-type", default="html", help="可视化类型")
    arg_parser.add_argument("--rank_list", type=str, help="Rank id list", default='all')
    args = arg_parser.parse_args()

    # Allocate profiling data
    data_map = allocate_prof_data(args.input_path)

    # Prepare parser configuration
    parser_params = {
        Constant.DATA_TYPE: Constant.TEXT,  # Default to TEXT type
        Constant.DATA_MAP: data_map,
        Constant.RANK_LIST: args.rank_list,
    }
    visualizer_params = {}

    parser_config = DictConfig(parser_params)
    visualizer_config = DictConfig(visualizer_params)

    # Get and call parser function
    json_parser = ClusterDataParser(parser_config)
    parser_fn = json_parser.get_cluster_parser_fn(args.profiler_type)
    parser_fn({})
    data = json_parser.get_data()


    # Call visualizer
    visualizer_fn = get_cluster_visualizer_fn(args.vis_type)
    visualizer_fn(data, args.output_path, visualizer_config)


if __name__ == "__main__":
    main()