package schema

import "strings"

var (
	uncountable = map[string]bool{
		"audio": true, "data": true, "equipment": true, "feedback": true, "information": true,
		"metadata": true, "money": true, "news": true, "rice": true, "series": true,
		"sheep": true, "species": true, "fish": true, "deer": true,
	}
	irregularPlural = map[string]string{
		"person": "people", "child": "children", "man": "men", "woman": "women",
		"mouse": "mice", "goose": "geese", "tooth": "teeth", "foot": "feet", "ox": "oxen",
		"alias": "aliases", "cache": "caches", "movie": "movies", "cookie": "cookies",
		"zombie": "zombies", "pie": "pies", "tie": "ties", "quiz": "quizzes",
	}
	irregularSingular = invert(irregularPlural)
	// latinUs lists -us nouns whose plural is -uses; other -uses words
	// (houses, causes) are an -use noun plus s.
	latinUs = map[string]bool{
		"status": true, "bus": true, "bonus": true, "campus": true, "virus": true,
		"census": true, "prospectus": true, "syllabus": true, "apparatus": true, "nexus": true,
	}
)

func invert(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[v] = k
	}
	return out
}

// lastWord splits a snake_case name into its prefix (with the trailing
// underscore) and its last word, which inflection applies to.
func lastWord(s string) (prefix, word string) {
	i := strings.LastIndexByte(s, '_')
	return s[:i+1], s[i+1:]
}

// pluralize forms the plural of a snake_case noun, for table names
// (Laravel's Str::plural): users, categories, statuses, people.
func pluralize(s string) string {
	prefix, w := lastWord(s)
	switch {
	case w == "" || uncountable[w]:
		return s
	case irregularPlural[w] != "":
		return prefix + irregularPlural[w]
	case strings.HasSuffix(w, "y") && len(w) > 1 && !isVowel(w[len(w)-2]):
		return prefix + w[:len(w)-1] + "ies"
	case strings.HasSuffix(w, "s"), strings.HasSuffix(w, "x"), strings.HasSuffix(w, "z"),
		strings.HasSuffix(w, "ch"), strings.HasSuffix(w, "sh"):
		return prefix + w + "es"
	}
	return prefix + w + "s"
}

// singular is the inverse of pluralize: categories -> category,
// statuses -> status, houses -> house, people -> person.
func singular(s string) string {
	prefix, w := lastWord(s)
	switch {
	case w == "" || uncountable[w]:
		return s
	case irregularSingular[w] != "":
		return prefix + irregularSingular[w]
	case strings.HasSuffix(w, "ies") && len(w) > 3 && !isVowel(w[len(w)-4]):
		return prefix + w[:len(w)-3] + "y"
	case strings.HasSuffix(w, "uses"):
		if stem := w[:len(w)-2]; latinUs[stem] {
			return prefix + stem
		}
		return prefix + w[:len(w)-1]
	case strings.HasSuffix(w, "sses"), strings.HasSuffix(w, "xes"), strings.HasSuffix(w, "zzes"),
		strings.HasSuffix(w, "ches"), strings.HasSuffix(w, "shes"):
		return prefix + w[:len(w)-2]
	case strings.HasSuffix(w, "ss"), strings.HasSuffix(w, "us"), strings.HasSuffix(w, "is"):
		return s
	case strings.HasSuffix(w, "s"):
		return prefix + w[:len(w)-1]
	}
	return s
}

func isVowel(c byte) bool { return c == 'a' || c == 'e' || c == 'i' || c == 'o' || c == 'u' }
