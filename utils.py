import subprocess
import matplotlib.pyplot as plt
from tqdm import tqdm
import csv


def run_process(name, no_iters=5, count=1_000_000,
                domains=16,
                no_warmup=None,
                validate=False,
                verbose=False,
                init_count=None,
                sorted=None,
                no_searches=None,
                search_threshold=None,
                search_par_threshold=None,
                insert_threshold=None,
                branching_factor=None,
                graph_nodes=None,
                expensive_searches=None
                ):
    name_args = name.split(" ")
    cmd = ["./_build/default/benchmark/bench.exe", *name_args, "-D", str(domains),
           "--no-iter", str(no_iters), "--count", str(count)]
    if validate:
        cmd += ["-T"]
    if init_count:
        cmd += ["--init-count", str(init_count)]
    if sorted:
        cmd += ["-s"]
    if no_searches:
        cmd += ["--no-searches", str(no_searches)]
    if search_threshold:
        cmd += ["--search-threshold", str(search_threshold)]
    if search_par_threshold:
        cmd += ["--search-par-threshold", str(search_par_threshold)]
    if insert_threshold:
        cmd += ["--insert-threshold", str(insert_threshold)]
    if branching_factor:
        cmd += ["--branching-factor", str(branching_factor)]
    if graph_nodes:
        cmd += ["--graph-nodes", str(graph_nodes)]
    if expensive_searches:
        cmd += ["--expensive-searches"]
    if no_warmup:
        cmd += ["--no-warmup", str(int(no_warmup))]
    result = subprocess.run(cmd, capture_output=True, check=True)
    stdout = result.stdout.decode("utf-8").splitlines()
    for output in stdout[:-1]:
        print(output)
    [time, _, var] = stdout[-1].split()
    time = time.removesuffix("s").strip()
    var = var.removesuffix("s").strip()
    if verbose:
        print(f"time for {name} with {count} inserts was {time} +- {var}")
    return float(time), float(var)


def run_test(op, args):
    if isinstance(op, str):
        res = run_process(op, **args)
    elif isinstance(op, dict):
        op_args = {key: op[key]
                   for key in op if key not in {'name', 'label', 'title'}}
        res = run_process(op['name'], **op_args, **args)
    else:
        raise ValueError(f'Invalid operation {op}')
    return res


def test_name(op):
    if isinstance(op, str):
        return op
    elif isinstance(op, dict) and 'title' in op:
        return op['title']
    else:
        raise ValueError(f'Invalid operation {op}')


def test_label(op):
    if isinstance(op, str):
        return op
    elif isinstance(op, dict) and 'label' in op:
        return op['label']
    else:
        raise ValueError(f'Invalid operation {op}')

def build_results(data_structures, args, param='domains', values=None):
    results = []
    if not values:
        values = range(1, 9)
    no_searches = args.get('no_searches', 0)
    count = args.get('count', 0)
    workload_size = float(no_searches + count)
    for i in tqdm(values):
        result = {param: i}
        for data_structure in data_structures:
            time, sd = run_test(data_structure, {param: i, **args})
            name = test_label(data_structure)
            result[name] = time
            result[name + "-throughput"] = workload_size / float(time)
            result[name + "-sd"] = sd
        results.append(result)
    return results


def build_results_seq_opt(data_structures, args, sequential=None,
                          param='domains', values=None):
    results = []
    if not values:
        values = range(1, 9)
    no_searches = args.get('no_searches', 0)
    count = args.get('count', 0)
    workload_size = float(no_searches + count)
    if sequential:
        seq_time, _ = run_test(sequential, {param: 1, **args})
        seq_throughput = workload_size / float(seq_time)
        seq_name = test_label(sequential)
    for i in tqdm(values):
        result = {param: i}
        if sequential:
            result[seq_name] = seq_time
            result[seq_name + "-throughput"] = seq_throughput
            result[seq_name + "-sd"] = 0
        for data_structure in data_structures:
            time, sd = run_test(data_structure, {param: i, **args})
            name = test_label(data_structure)
            result[name] = time
            result[name + "-throughput"] = workload_size / float(time)
            result[name + "-sd"] = sd
        results.append(result)
    return results


def plot_results(param, data_structures, results, title=None, xlabel=None):
    if not title:
        title = f"Comparison of {param} values on data structure"
    if not xlabel:
        xlabel = param
    param_values = [data[param] for data in results]
    _ = plt.figure(figsize=(12, 8), dpi=100, facecolor='w', edgecolor='k')
    for data_structure in data_structures:
        label = test_label(data_structure)
        name = test_name(data_structure)
        values = [data[label] for data in results]
        err = [data[label + "-sd"] for data in results]
        plt.errorbar(param_values, values, yerr=err, label=name)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel('Time (s)')
    plt.legend()
    plt.show() 


def plot_throughput_results(param, data_structures, results,
                            title=None, xlabel=None):
    if not title:
        title = f"Comparison of {param} values on data structure"
    if not xlabel:
        xlabel = param
    param_values = [data[param] for data in results]
    _ = plt.figure(figsize=(12, 8), dpi=100, facecolor='w', edgecolor='k')
    for data_structure in data_structures:
        label = test_label(data_structure)
        name = test_name(data_structure)
        values = [data[label + "-throughput"] for data in results]
        plt.errorbar(param_values, values, label=name)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel('Throughput (ops/sec)')
    plt.legend()
    plt.show() 


def interactive_plot():
    fig = plt.figure(figsize=(8, 6), dpi=100, facecolor='w', edgecolor='k')
    ax = fig.add_subplot(111)
    plt.ion()
    return fig, ax


def build_interactive_plot(fig, ax, data_structures, params={}, title=None,
                           xlabel=None, param=None, values=None):
    if not param:
        param = 'domains'
    if not values:
        values = range(1, 9)
    if not title:
        title = f"Comparison on value of {param} on data structure"
    if not xlabel:
        xlabel = param

    no_searches = params.get('no_searches', 0)
    count = params.get('count', 0)
    workload_size = float(no_searches + count)

    times = []
    results = []
    for i in values:
        result = {param: i}
        times.append(i)
        results.append(result)
        for data_structure in data_structures:
            t, var = run_test(data_structure, {param: i, **params})
            label = test_label(data_structure)
            result[label] = t
            result[label+'-throughput'] = workload_size/t
            result[label+'-sd'] = var

            ax.clear()
            ax.set_title(title)
            ax.set_xlabel(xlabel)
            ax.set_ylabel('Time (s)')
            for data_structure in data_structures:
                label = test_label(data_structure)
                name = test_name(data_structure)
                available_values =\
                    [data[label] for data in results if label in data]
                available_var = [
                    data[label+'-sd']
                    for data in results if (label + '-sd') in data]
                available_times = times[:len(available_values)]
                ax.errorbar(available_times, available_values,
                            yerr=available_var, label=name)
            ax.legend()
            fig.canvas.draw()
    return times, results


def dump_results_to_csv(results, file_name):
    with open(f'{file_name}.csv', 'w', newline='') as f:
        fieldnames = list(results[0].keys())
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        sequential_key = None
        sequential_value = None

        sequential_throughput_key = None
        sequential_throughput_value = None

        sequential_sd_key = None
        sequential_sd_value = None

        for field in fieldnames:
            if field.endswith("sequential"):
                sequential_key = field
                break
            if field.endswith("sequential-throughput"):
                sequential_throughput_key = field
                break
            if field.endswith("sequential-sd"):
                sequential_sd_key = field
                break

        for row in results:
            row = row.copy()
            if sequential_key:
                if not sequential_value:
                    sequential_value = row[sequential_key]
                row[sequential_key] = sequential_value
            if sequential_throughput_key:
                if not sequential_throughput_value:
                    sequential_throughput_value =\
                        row[sequential_throughput_key]
                row[sequential_throughput_key] = sequential_throughput_value
            if sequential_sd_key:
                if not sequential_sd_value:
                    sequential_sd_value = row[sequential_sd_key]
                row[sequential_sd_key] = sequential_sd_value

            writer.writerow(row)
