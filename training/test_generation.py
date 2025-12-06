import random

def select_from_xor(selection):
    get_count = len(selection)
    choice_idx = selection[random.randrange(get_count)]
    return selection[choice_idx]


def get_arg2prompt(args_type, args_dict, required, p=0.5):
    
    # 1. Select an argument and confirm it exists
    if "XOR" in args_type:
        selection = []
        for arg_name, arg_data in args_dict[args_type].items():
            if not arg_data["associated_entity"]: # will have to modify this
                continue
            else:
                selection.append(arg_name)
        if not selection:
            if required: return -2, "" # No selection of required field was possible
            else: return -1, "" # No selection of non-required field was possible
        
        selected_arg = select_from_xor(selection)
        selected_arg_data = args_dict[args_type][selected_arg]
    
    else: # So an argument has to be inlcuded
        selected_arg = args_type
        selected_arg_data = args_dict[selected_arg]
        if required:
            if not selected_arg_data["associated_entity"]: # will have to modify this
                return -2, "" # No selection of required field was possible
        else: # not required
            if random.random() < p:
                return 0, "" # Randomly decided to to include non-required argument
            else:
                if not selected_arg_data["associated_entity"]: # will have to modify this
                    return -1, "" # No selection of non-required field was possible
    
    
    # 2. Generate a prompt of the argument
    queriable_fields = list(selected_arg_data["queriable_fields"].keys())
    selected_field = select_from_xor(queriable_fields)
    entities_ids = # list of ids of required ids just need to query DB for this.
    selected_entity = select_from_xor(entities_ids)
    entity_field = str(selected_entity.selected_field) # again query from the DB. This will have to be changed to account for differnet possible data types. Assume they can be queried from the prompt generation encoding.
    prompt = selected_arg_data["queriable_fields"][selected_field]["prompt"] + entity_field 
    return 0, prompt
    
    
   
def create_aciton_prompt(action, p):
    prompt = ""
    
    required_args = action["required_args"]
    for required_arg_type in required_args:
        status, prompt_addition = get_arg2prompt(required_arg_type, required_args, required=True)
        
        if status == -2:
            raise ValueError("No entity for a required feild exists.")
        else:
            prompt += prompt_addition
    
    nonrequired_args = action["nonrequired_args"]
    for nonrequired_arg_type in action["nonrequired_args"]:
        status, prompt_addition = get_arg2prompt(nonrequired_arg_type, nonrequired_args, required=False)
        
        if status == -2:
            raise ValueError("No entity for a required feild exists.")
        else:
            prompt += prompt_addition
    
    return prompt


######### SAMPLE PROMPT ENCODING STRUCTURE ################
# I will only add neccessary fields for the operation (assumingly of the algorithm above)

# Revised encoding example (separate schema from data, explicit XOR grouping and sampling hints)
action = {
    "action_name": "issueCreate",
    # Base instruction to start the prompt; arg prompt_fragments get appended after sampling
    "prompt_intro": "Create an issue.",
    "args": [
        {
            "name": "teamId",
            "required": True,
            "group": "xor_team",             # mutually exclusive choices resolved by sampler
            "entity_type": "teams",          # lookup catalog (e.g., seeded teams)
            "field": "id",
            "sample_strategy": "random_from_catalog",
            "prompt_fragment": "Use team {teamId}. "
        },
        {
            "name": "assigneeId",
            "required": False,
            "entity_type": "users",
            "field": "id",
            "weight": 0.7,                   # probability of inclusion for optional arg
            "sample_strategy": "random_from_catalog",
            "prompt_fragment": "Assign to {assigneeId}. "
        },
        {
            "name": "title",
            "required": True,
            "sample_strategy": "template",
            "template": "Hydration chain regression {rand_int}",
            "constraints": { "max_len": 140 },
            "prompt_fragment": "Title it '{title}'. "
        },
        {
            "name": "description",
            "required": False,
            "sample_strategy": "template",
            "template": "Investigate issue {rand_int} found during agent run.",
            "weight": 0.5,
            "prompt_fragment": "Add description: {description}. "
        },
        {
            "name": "priority",
            "required": False,
            "sample_strategy": "choice",
            "choices": [0, 1, 2, 3, 4],
            "constraints": { "type": "int" },
            "weight": 0.8,
            "prompt_fragment": "Set priority to {priority}. "
        }
    ],
    # Final prompt is assembled by concatenating the action name + each arg's prompt_fragment
    "expected_diff": [
        {
            "diff_type": "added",
            "entity": "issues",
            "where": {
                "teamId": { "eq": "{teamId}" },
                "title": { "contains": "{title}" },
                "assigneeId": { "eq": "{assigneeId}" }
            },
            "expected_count": 1
        }
    ]
}

# -------------------------------------------------------------------------
# New sampler utilities for the revised encoding (non-destructive)
# -------------------------------------------------------------------------

def _pick_from_catalog(arg, catalogs):
    """Sample a value according to an argument's sampling strategy."""
    strategy = arg.get("sample_strategy", "random_from_catalog")
    if strategy == "random_from_catalog":
        pool = catalogs.get(arg.get("entity_type"), [])
        return random.choice(pool) if pool else None
    if strategy == "template":
        tmpl = arg.get("template", "")
        return tmpl.format(rand_int=random.randint(1, 9999))
    if strategy == "choice":
        choices = arg.get("choices", [])
        return random.choice(choices) if choices else None
    return None


def sample_args_and_prompt_v2(action_def, catalogs, default_weight=0.5):
    """
    Generate a prompt and argument map from the revised_action schema.

    - Respects XOR-style grouping via shared `group` names (one arg per group).
    - Optional args are included based on `weight` (or default_weight).
    - Prompt is assembled from prompt_intro + each included arg's prompt_fragment.
    """
    prompt_parts = [action_def.get("prompt_intro", "")]
    arg_values = {}
    chosen_groups = set()

    for arg in action_def.get("args", []):
        group = arg.get("group")
        if group:
            if group in chosen_groups:
                continue
            chosen_groups.add(group)

        if not arg.get("required", False):
            weight = arg.get("weight", default_weight)
            if random.random() > weight:
                continue

        value = _pick_from_catalog(arg, catalogs)
        if value is None:
            if arg.get("required", False):
                raise ValueError(f"No value available for required arg {arg['name']}")
            else:
                continue

        arg_values[arg["name"]] = value

        frag = arg.get("prompt_fragment", "")
        if frag:
            try:
                prompt_parts.append(frag.format(**arg_values))
            except KeyError:
                prompt_parts.append(frag)

    prompt = " ".join(prompt_parts).strip()
    return prompt, arg_values
