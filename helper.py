import json

categories = ()


def create_categories(lines, curr_sequence):
    curr_sequence = curr_sequence - len(lines)
    global categories
    places_categories = ()
    for line in lines:
        if not line[18] or not line[19]:
            continue

        category_ids = json.loads(line[18])
        category_lables = json.loads(line[19])
        if len(category_ids) == 0:
            continue

        for i in range(0, len(category_ids)):
            category = (category_ids[i], category_lables[i])
            if category not in categories:
                categories += (category,)

            curr_sequence = curr_sequence + 1
            place_category = (curr_sequence, category_ids[i])
            places_categories += (place_category,)

    return places_categories
