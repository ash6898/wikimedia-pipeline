CREATE TABLE IF NOT EXISTS wiki_window_counts (
    id SERIAL PRIMARY KEY,
    wiki TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    edit_count INTEGER NOT NULL,
    inserted_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (wiki, window_start)
);
CREATE INDEX IF NOT EXISTS wiki_window_counts_wiki_window_idx
    ON wiki_window_counts (wiki, window_start);

-- One row per wiki. Spark stores the wiki's total article edits from
-- wiki_window_counts. editor and is_bot are unused (null).
CREATE TABLE IF NOT EXISTS top_editors (
    id SERIAL PRIMARY KEY,
    wiki TEXT,
    editor TEXT,
    edit_count INTEGER,
    is_bot BOOLEAN,
    computed_at TIMESTAMPTZ DEFAULT NOW()
);

-- bot_ratio = bot_edits / (bot_edits + human_edits) for namespace 0 edits
-- in the epoch-aligned 60s event-time window [window_start, window_end).
-- Written by the PySpark batch job from the raw wiki-edits topic.
CREATE TABLE IF NOT EXISTS bot_ratios (
    id SERIAL PRIMARY KEY,
    wiki TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    bot_edits INTEGER NOT NULL,
    human_edits INTEGER NOT NULL,
    bot_ratio DOUBLE PRECISION NOT NULL,
    computed_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (wiki, window_start)
);
CREATE INDEX IF NOT EXISTS bot_ratios_wiki_window_idx
    ON bot_ratios (wiki, window_start);
