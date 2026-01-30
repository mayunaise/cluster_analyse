import argparse
from ast import arg
import os
from parser import get_cluster_parser_fn, RayContext
from visualizer import get_cluster_visualizer_fn
from omegaconf import DictConfig
from constant import Constant
from data_preprocessor import DataPreprocessor

try:
    import ray
    RAY_AVAILABLE = True
except ImportError:
    RAY_AVAILABLE = False

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
    arg_parser.add_argument("--use-ray", action="store_true", help="使用 Ray 进行并行处理")
    args = arg_parser.parse_args()

    # Allocate profiling data
    data_map = allocate_prof_data(args.input_path)

    # Prepare parser configuration
    parser_params = {
        Constant.DATA_TYPE: Constant.TEXT,  # Default to TEXT type
        Constant.DATA_MAP: data_map,
        Constant.RECIPE_NAME: "",
        Constant.RANK_LIST: args.rank_list,
    }
    parser_config = DictConfig(parser_params)
    visualizer_config = DictConfig({})

    # Initialize RayContext (if parallel processing is requested)
    context = None
    if args.use_ray:
        if not RAY_AVAILABLE:
            print("Warning: Ray is not available. Falling back to serial processing.")
        else:
            context = RayContext()

    # Get and call parser function
    parser_fn = get_cluster_parser_fn(args.profiler_type, params=parser_params)
    parser_fn(context=context)

    # Get parsed data
    from parser import _get_parser_instance
    parser_instance = _get_parser_instance()
    data = parser_instance.get_data()

    # Call visualizer
    visualizer_fn = get_cluster_visualizer_fn(args.vis_type)
    visualizer_fn(data, args.output_path, visualizer_config)


if __name__ == "__main__":
    main()