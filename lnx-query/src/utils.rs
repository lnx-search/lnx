use std::collections::HashMap;

/// A simple levenshtein distance calculator.
pub fn damerau_levenshtein(s: &str, t: &str) -> usize {
    // get length of unicode chars
    let len_s = s.chars().count();
    let len_t = t.chars().count();
    let max_distance = len_t + len_s;

    // initialize the matrix
    let mut mat: Vec<Vec<usize>> = vec![vec![0; len_t + 2]; len_s + 2];
    mat[0][0] = max_distance;
    for i in 0..(len_s + 1) {
        mat[i + 1][0] = max_distance;
        mat[i + 1][1] = i;
    }
    for i in 0..(len_t + 1) {
        mat[0][i + 1] = max_distance;
        mat[1][i + 1] = i;
    }

    let mut char_map: HashMap<char, usize> = HashMap::new();
    // apply edit operations
    for (i, s_char) in s.chars().enumerate() {
        let mut db = 0;
        let i = i + 1;

        for (j, t_char) in t.chars().enumerate() {
            let j = j + 1;
            let last = *char_map.get(&t_char).unwrap_or(&0);

            let cost = if s_char == t_char { 0 } else { 1 };
            mat[i + 1][j + 1] = *[
                mat[i + 1][j] + 1,                                 // deletion
                mat[i][j + 1] + 1,                                 // insertion
                mat[i][j] + cost,                                  // substitution
                mat[last][db] + (i - last - 1) + 1 + (j - db - 1), // transposition
            ]
            .iter()
            .min()
            .unwrap();

            // that's like s_char == t_char but more efficient
            if cost == 0 {
                db = j;
            }
        }

        char_map.insert(s_char, i);
    }

    mat[len_s + 1][len_t + 1]
}
