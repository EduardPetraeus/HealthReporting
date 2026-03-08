-- Chat history schema for multi-turn conversations.
-- Stores user/assistant/tool messages per session for context continuity.

CREATE SCHEMA IF NOT EXISTS agent;

CREATE TABLE IF NOT EXISTS agent.chat_history (
    id VARCHAR PRIMARY KEY,
    session_id VARCHAR NOT NULL,
    role VARCHAR NOT NULL,  -- 'user', 'assistant', 'tool_use', 'tool_result'
    content TEXT NOT NULL,
    tool_name VARCHAR,
    tool_input TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_chat_history_session
ON agent.chat_history(session_id, created_at);
