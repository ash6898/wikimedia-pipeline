CREATE TABLE IF NOT EXISTS wiki_window_counts (
    id SERIAL PRIMARY KEY,
    wiki TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    edit_count INTEGER NOT NULL,
    inserted_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX ON wiki_window_counts (wiki, window_start);

CREATE TABLE IF NOT EXISTS top_editors (
    id SERIAL PRIMARY KEY,
    wiki TEXT,
    editor TEXT,
    edit_count INTEGER,
    is_bot BOOLEAN,
    computed_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bot_ratios (
    id SERIAL PRIMARY KEY,
    wiki TEXT,
    window_start TIMESTAMPTZ,
    window_end TIMESTAMPTZ,
    bot_edits INTEGER,
    human_edits INTEGER,
    computed_at TIMESTAMPTZ DEFAULT NOW()
);