#!/usr/bin/env python3
"""Capture real pinned SDK/CLI wire requests against loopback only; no real model calls."""
from __future__ import annotations
import argparse
import asyncio
from contextlib import contextmanager, nullcontext
import hashlib
import json
import os
from pathlib import Path
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import tempfile
import threading
from unittest.mock import patch

from xinghuo.veria.driver import VeriaRoleDriver
from xinghuo.veria.prompts import build_role_prompt


class OfflineEnvironment:
    _description = {'execution_mode': 'hosted_process', 'network_mode': 'none', 'web_tools': []}
    accepting_commands = True
    def __init__(self):
        self.executed = []
    async def prepare_command(self, member, command, tool_id):
        if command != 'printf offline-wire-tool':
            raise ValueError('Loopback fixture accepts only its fixed benign command')
        self.executed.append(command)
        return 'printf offline-wire-tool'
    def complete_command(self, *_):
        pass
    async def stop_member(self, *_):
        pass


@contextmanager
def fixture_server(messages_path="/v1/messages", *, structured=False):
    captured = []
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass
        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers.get('Content-Length', '0'))))
            captured.append({'path': self.path, 'body': body, 'auth': self.headers.get('x-api-key') or self.headers.get('authorization')})
            if self.path.split('?')[0] != messages_path:
                data = b'{}'
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(data)))
                self.end_headers()
                self.wfile.write(data)
                return
            has_result = any(isinstance(m.get('content'), list) and any(b.get('type') == 'tool_result' for b in m['content']) for m in body.get('messages', []))
            use_structured = structured and has_result and any(tool.get('name') == 'StructuredOutput' for tool in body.get('tools', []))
            model = body['model']
            events = [('message_start', {'type': 'message_start', 'message': {'id': 'msg_fixture', 'type': 'message', 'role': 'assistant', 'model': model, 'content': [], 'stop_reason': None, 'stop_sequence': None, 'usage': {'input_tokens': 1, 'output_tokens': 0}}})]
            if not has_result:
                events.extend([
                    ('content_block_start', {'type': 'content_block_start', 'index': 0, 'content_block': {'type': 'tool_use', 'id': 'fixture_tool', 'name': 'Bash', 'input': {}}}),
                    ('content_block_delta', {'type': 'content_block_delta', 'index': 0, 'delta': {'type': 'input_json_delta', 'partial_json': json.dumps({'command': 'printf offline-wire-tool'})}})])
            elif use_structured:
                events.extend([
                    ('content_block_start', {'type': 'content_block_start', 'index': 0, 'content_block': {'type': 'tool_use', 'id': 'fixture_structured', 'name': 'StructuredOutput', 'input': {}}}),
                    ('content_block_delta', {'type': 'content_block_delta', 'index': 0, 'delta': {'type': 'input_json_delta', 'partial_json': json.dumps({'type': 'flag_found', 'flag': '', 'method': 'offline fixture; no acquired flag'})}})])
            else:
                events.extend([
                    ('content_block_start', {'type': 'content_block_start', 'index': 0, 'content_block': {'type': 'text', 'text': ''}}),
                    ('content_block_delta', {'type': 'content_block_delta', 'index': 0, 'delta': {'type': 'text_delta', 'text': 'offline-ok'}})])
            events.extend([
                ('content_block_stop', {'type': 'content_block_stop', 'index': 0}),
                ('message_delta', {'type': 'message_delta', 'delta': {'stop_reason': 'tool_use' if (not has_result or use_structured) else 'end_turn', 'stop_sequence': None}, 'usage': {'output_tokens': 1}}),
                ('message_stop', {'type': 'message_stop'})])
            data = ''.join(f'event: {name}\ndata: {json.dumps(value)}\n\n' for name, value in events).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'text/event-stream')
            self.send_header('Content-Length', str(len(data)))
            self.end_headers()
            self.wfile.write(data)
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f'http://127.0.0.1:{server.server_port}', captured
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


async def verify(model, endpoint, captured, thinking_budget=None, *, inline_control=False):
    from claude_agent_sdk import ClaudeAgentOptions
    def experimental_options(**kwargs):
        if inline_control:
            # Reproduce the old transport only for this synthetic loopback A/B
            # control, to show file mode does not add a default prompt layer.
            kwargs['system_prompt'] = Path(kwargs['system_prompt']['path']).read_text()
        if thinking_budget is not None:
            kwargs['thinking'] = {'type': 'enabled', 'budget_tokens': thinking_budget}
        return ClaudeAgentOptions(**kwargs)
    events = []
    environment = OfflineEnvironment()
    async def tool(*_):
        return {'messages': []}
    with (patch('claude_agent_sdk.ClaudeAgentOptions', side_effect=experimental_options) if thinking_budget is not None or inline_control else nullcontext()), tempfile.TemporaryDirectory(prefix='glm-wire-') as temp, patch.dict(os.environ, {
        'XINGHUO_PROVIDER': 'glm', 'GLM_API_KEY': 'synthetic-offline-key', 'GLM_BASE_URL': endpoint,
        'ANTHROPIC_API_KEY': 'synthetic-offline-key', 'ANTHROPIC_AUTH_TOKEN': 'synthetic-offline-key',
        'CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC': '1', 'CLAUDECODE': '',
    }):
        # A target and a curl example deliberately appear in the audited prompt.
        # They must reach the model verbatim while staying out of process argv,
        # where broad process cleanup could otherwise match every SDK client.
        context = {'challenge': {'connection_info': 'http://192.0.2.42/',
                   'description': 'Fixture target: curl http://192.0.2.42/; do not connect in this offline test.'}}
        capture_start = len(captured)
        driver = VeriaRoleDriver('coordinator', 'coordinator', model, Path(temp), context, environment, tool,
            on_event=lambda name, data: events.append((name, data)))
        try:
            await driver.start()
            expected = build_role_prompt('coordinator', driver.tool_member, context, environment._description)
            prompt_file = driver.workspace / 'sdk-system-prompt.txt'
            assert prompt_file.read_text() == expected
            process = driver.client._transport._process
            ps = await asyncio.create_subprocess_exec('ps', '-p', str(process.pid), '-o', 'command=',
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            argv_bytes, _ = await ps.communicate()
            argv = argv_bytes.decode()
            assert ps.returncode == 0 and argv, 'Cannot inspect actual running CLI argv'
            if inline_control:
                assert '--system-prompt ' in argv and '192.0.2.42' in argv, 'Old transport control did not expose fixture text'
            else:
                assert '--system-prompt-file' in argv, argv
                assert str(prompt_file) in argv, argv
                assert '192.0.2.42' not in argv and expected not in argv, 'Prompt content leaked into CLI argv'
                assert '--system-prompt ' not in argv and '--append-system-prompt' not in argv, argv
            result = await asyncio.wait_for(driver.turn('Run the fixed offline fixture command, then report offline-ok.'), 45)
            assert result['result'] == 'offline-ok', result
            assert not result['is_error'], result
            assert environment.executed == ['printf offline-wire-tool'], environment.executed
            messages = [r['body'] for r in captured[capture_start:]
                        if r['path'].split('?')[0] == '/v1/messages']
            assert len(messages) >= 2, 'No real tool continuation request was captured'
            wire_systems = []
            for body in messages:
                system = body.get('system', [])
                texts = [system] if isinstance(system, str) else [b.get('text') for b in system if b.get('type') == 'text']
                assert texts.count(expected) == 1, 'Builder prompt missing, changed or duplicated in API system'
                sdk_blocks = [text for text in texts if text != expected]
                assert len(sdk_blocks) == 2 and sdk_blocks[0].startswith('x-anthropic-billing-header: '), sdk_blocks
                assert sdk_blocks[1] == "You are a Claude agent, built on Anthropic's Claude Agent SDK.", sdk_blocks
                wire_systems.append(hashlib.sha256(json.dumps(texts, ensure_ascii=False).encode()).hexdigest())
            assert len(set(wire_systems)) == 1, 'System prompt changed during tool continuation'
            return {'model': model, 'result': result['result'], 'tools_executed': environment.executed,
                    'turn_success': True, 'events': [name for name, _ in events],
                    'system_prompt_sha256': hashlib.sha256(expected.encode()).hexdigest(),
                    'system_prompt_characters': len(expected), 'wire_system_matches_builder_verbatim': True,
                    'cli_argv_uses_prompt_file_only': not inline_control, 'wire_request_count': len(messages),
                    'wire_system_blocks_sha256': wire_systems[0], 'sdk_intrinsic_system_blocks': sdk_blocks}
        finally:
            await driver.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--thinking-budget', type=int, help='Experiment only: override SDK options without changing production driver')
    parser.add_argument('--compare-inline', action='store_true', help='Also capture the old inline transport as a synthetic A/B control')
    parser.add_argument('--models', nargs='+', default=['glm-5.3', 'glm-5.3-flash'], help='Model IDs to inspect against the synthetic endpoint; no provider is contacted')
    args = parser.parse_args()
    summary = {'real_model_calls': 0, 'model_endpoint': '127.0.0.1 only', 'fixture': 'synthetic Anthropic SSE; real SDK and CLI + real PreToolUse/PostToolUse hooks'}
    with fixture_server() as (endpoint, captured):
        try:
            summary['runs'] = [asyncio.run(verify(model, endpoint, captured, args.thinking_budget)) for model in args.models]
            if args.compare_inline:
                control = asyncio.run(verify(args.models[0], endpoint, captured, args.thinking_budget, inline_control=True))
                summary['inline_control'] = control
                assert control['wire_system_blocks_sha256'] == summary['runs'][0]['wire_system_blocks_sha256'], 'Prompt-file mode changed API system blocks'
                summary['inline_and_file_wire_system_identical'] = True
            summary['passed'] = True
        except BaseException as exc:
            summary['passed'] = False
            summary['error'] = f'{type(exc).__name__}: {exc}'
        requests = [r['body'] for r in captured if r['path'].split('?')[0] == '/v1/messages']
        summary['requests'] = [{
            'model': body.get('model'), 'thinking': body.get('thinking'),
            'max_tokens': body.get('max_tokens'), 'stream': body.get('stream'),
            'output_config': body.get('output_config'),
            'tool_names': [tool.get('name') for tool in body.get('tools', [])],
            'tool_result_count': sum(1 for message in body.get('messages', []) if isinstance(message.get('content'), list) for block in message['content'] if block.get('type') == 'tool_result'),
        } for body in requests]
        summary['no_disabled_thinking'] = all((body.get('thinking') or {}).get('type') != 'disabled' for body in requests)
        summary['no_foreign_model'] = all(body.get('model') in args.models for body in requests)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary['passed'] and summary['no_disabled_thinking'] and summary['no_foreign_model'] else 1

if __name__ == '__main__':
    raise SystemExit(main())
