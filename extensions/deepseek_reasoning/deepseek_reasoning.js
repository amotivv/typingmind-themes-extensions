(function() {
  /**
   * ------------------------------------------------------
   * 1. Preserve the original fetch for fallback behavior.
   * ------------------------------------------------------
   */
  const originalFetch = window.fetch;

  /**
   * ------------------------------------------------------
   * 2. API endpoints to intercept.
   * ------------------------------------------------------
   */
  const TARGET_API_URLS = [
    'openrouter.ai/api/v1/chat/completions',
    'api.together.xyz/v1/chat/completions'
  ];

  /**
   * ------------------------------------------------------
   * Helper Function: Check if a given URL is one of the target endpoints.
   * @param {string} url - The request URL (args[0] from fetch).
   * @returns {boolean} - True if the URL includes one of the target API endpoints.
   * ------------------------------------------------------
   */
  function isTargetURL(url) {
    return url && TARGET_API_URLS.some(apiUrl => url.includes(apiUrl));
  }

  /**
   * ------------------------------------------------------
   * Helper Function: Check if the request body references a Deepseek model.
   * @param {Object} body - Parsed JSON request body.
   * @returns {boolean} - True if the model field includes 'deepseek'.
   * ------------------------------------------------------
   */
  function isDeepseekModel(body) {
    return !!body.model && body.model.includes('deepseek');
  }

  /**
   * ------------------------------------------------------
   * Helper Function: Check if the request is a title generation request.
   * @param {Object} body - Parsed JSON request body.
   * @returns {boolean} - True if the last message prompts for a short/relevant title.
   * ------------------------------------------------------
   */
  function isTitleRequest(body) {
    if (!body.messages || body.messages.length === 0) return false;
    const lastMessage = body.messages[body.messages.length - 1];
    return (
      !!lastMessage.content &&
      lastMessage.content.startsWith('What would be a short and relevant title for this chat?')
    );
  }

  /**
   * ------------------------------------------------------
   * Function: Process a streaming text/event-stream response from Deepseek,
   *   intercepting "reasoning_content" and measuring how long
   *   the "thinking" phase lasted.
   * @param {Response} response - The original response from fetch.
   * @returns {Promise<Response>}
   * ------------------------------------------------------
   */
  async function processStreamingResponse(response) {
    const reader = response.body.getReader();
    const decoder = new TextDecoder();

    // We'll keep track of partial lines in `buffer` until a newline is encountered.
    let buffer = '';
    let reasoningStarted = false;
    let reasoningEnded = false;
    let startThinkingTime = null;
    let accumulatedContent = '';  // For accumulating content between think tags
    let thinkTagOpened = false;   // Track if we're inside think tags
    let headerShown = false;      // Track if we've shown the thinking header

    // Create a new ReadableStream that we will push modified data into.
    const stream = new ReadableStream({
      async start(controller) {
        const textEncoder = new TextEncoder();

        try {
          while (true) {
            const { done, value } = await reader.read();
            if (done) break;

            buffer += decoder.decode(value, { stream: true });
            const lines = buffer.split('\n');
            buffer = lines.pop() || '';

            for (const line of lines) {
              // Only parse lines that begin with 'data: '
              if (line.startsWith('data: ')) {
                // If we get '[DONE]', pass it along and continue
                if (line.includes('[DONE]')) {
                  controller.enqueue(textEncoder.encode(`${line}\n`));
                  continue;
                }

                try {
                  const data = JSON.parse(line.slice(6));

                  // We look for data.choices[0].delta
                  if (data?.choices?.[0]?.delta) {
                    const delta = data.choices[0].delta;

                    // Handle OpenRouter's reasoning field
                    if (delta.reasoning && delta.content === null) {
                      reasoningStarted = true;
                      if (!headerShown) {
                        startThinkingTime = performance.now();
                        const thinkingHeader = {
                          ...data,
                          choices: [{
                            ...data.choices[0],
                            delta: { content: '💭 Thinking...\n\n> ' }
                          }]
                        };
                        controller.enqueue(
                          textEncoder.encode(`data: ${JSON.stringify(thinkingHeader)}\n\n`)
                        );
                        headerShown = true;
                      }
                      // Format and send reasoning content
                      const content = delta.reasoning.replace(/\n/g, '\n> ');
                      const modifiedData = {
                        ...data,
                        choices: [{
                          ...data.choices[0],
                          delta: { content }
                        }]
                      };
                      controller.enqueue(
                        textEncoder.encode(`data: ${JSON.stringify(modifiedData)}\n\n`)
                      );
                      continue;
                    }

                    // Handle Together.ai's think tags
                    if (delta.content) {
                      // Opening think tag
                      if (delta.content.includes('<think>')) {
                        thinkTagOpened = true;
                        reasoningStarted = true;
                        startThinkingTime = performance.now();
                        // Remove opening tag and start accumulating
                        accumulatedContent = delta.content.replace('<think>', '');
                        if (!headerShown) {
                          const thinkingHeader = {
                            ...data,
                            choices: [{
                              ...data.choices[0],
                              delta: { content: '💭 Thinking...\n\n> ' }
                            }]
                          };
                          controller.enqueue(
                            textEncoder.encode(`data: ${JSON.stringify(thinkingHeader)}\n\n`)
                          );
                          headerShown = true;
                        }
                        continue;
                      }
                      
                      // Closing think tag
                      if (thinkTagOpened && delta.content.includes('</think>')) {
                        thinkTagOpened = false;
                        reasoningEnded = true;
                        // Get content before closing tag
                        const beforeClosing = delta.content.split('</think>')[0];
                        accumulatedContent += beforeClosing;
                        
                        // Format and send accumulated content
                        const content = accumulatedContent.replace(/\n/g, '\n> ');
                        const modifiedData = {
                          ...data,
                          choices: [{
                            ...data.choices[0],
                            delta: { content }
                          }]
                        };
                        controller.enqueue(
                          textEncoder.encode(`data: ${JSON.stringify(modifiedData)}\n\n`)
                        );

                        // Calculate and show thinking duration
                        const thinkingDuration = Math.round(
                          (performance.now() - startThinkingTime) / 1000
                        );
                        const separatorData = {
                          ...data,
                          choices: [{
                            ...data.choices[0],
                            delta: {
                              content: `\n\n💡 Thought for ${thinkingDuration} seconds\n\n---\n\n`
                            }
                          }]
                        };
                        controller.enqueue(
                          textEncoder.encode(`data: ${JSON.stringify(separatorData)}\n\n`)
                        );
                        
                        // Reset accumulation
                        accumulatedContent = '';
                        
                        // Get content after closing tag
                        const afterClosing = delta.content.split('</think>')[1]?.trim();
                        if (afterClosing) {
                          // Send remaining content
                          const remainingData = {
                            ...data,
                            choices: [{
                              ...data.choices[0],
                              delta: { content: afterClosing }
                            }]
                          };
                          controller.enqueue(
                            textEncoder.encode(`data: ${JSON.stringify(remainingData)}\n\n`)
                          );
                        }
                        continue;
                      }
                      
                      // Accumulate content between tags
                      if (thinkTagOpened) {
                        accumulatedContent += delta.content;
                        continue;
                      }

                      // Pass through normal content
                      if (!thinkTagOpened) {
                        const contentData = {
                          ...data,
                          choices: [{
                            ...data.choices[0],
                            delta: { content: delta.content }
                          }]
                        };
                        controller.enqueue(
                          textEncoder.encode(`data: ${JSON.stringify(contentData)}\n\n`)
                        );
                        continue;
                      }
                    }
                  } else {
                    // If there's no delta object, just pass through
                    controller.enqueue(textEncoder.encode(`${line}\n`));
                  }
                } catch (parseError) {
                  console.error('Error parsing streaming data:', parseError);
                  controller.enqueue(textEncoder.encode(`${line}\n`));
                }
              } else {
                // If line doesn't start with 'data: ', pass it as-is
                controller.enqueue(textEncoder.encode(`${line}\n`));
              }
            }
          }

          // Once the stream is fully read, close the controller
          controller.close();
        } catch (error) {
          controller.error(error);
        }
      }
    });

    // Return a new Response that wraps our transformed stream
    return new Response(stream, {
      headers: response.headers,
      status: response.status,
      statusText: response.statusText
    });
  }

  /**
   * ------------------------------------------------------
   * Function: Process a non-streaming JSON response from Deepseek.
   * If we see 'reasoning_content' in the JSON body, prefix each line
   * with '>' and insert a separator (---) before the original content.
   * @param {Response} response - The original fetch response.
   * @returns {Promise<Response>}
   * ------------------------------------------------------
   */
  async function processNonStreamingResponse(response) {
    try {
      // Clone the response so we can parse it independently
      const cloned = response.clone();
      const data = await cloned.json();

      // Look for reasoning content in the first choice (either in reasoning field or think tags)
      const message = data.choices[0].message;
      let reasoningContent = message.reasoning;
      
      // If no reasoning field, try to extract from think tags
      if (!reasoningContent && message.content) {
        const match = message.content.match(/<think>([\s\S]*?)<\/think>/);
        if (match) {
          reasoningContent = match[1].trim();
          // Remove the think tags from content
          message.content = message.content.replace(/<think>[\s\S]*?<\/think>\n*/, '').trim();
        }
      }

      if (reasoningContent) {
        const quotedReasoning = reasoningContent
          .split('\n')
          .map(line => (line.trim() ? `> ${line}` : '>'))
          .join('\n');

        // Insert reasoning before the main content
        message.content = `${quotedReasoning}\n\n---\n\n${message.content}`;

        // Return a new Response with the updated JSON
        return new Response(JSON.stringify(data), {
          status: response.status,
          statusText: response.statusText,
          headers: response.headers
        });
      }
    } catch (error) {
      console.error('Error in Deepseek reasoning extension (non-streaming):', error);
    }

    // If no modifications are needed, return the original response
    return response;
  }

  /**
   * ------------------------------------------------------
   * Main override: window.fetch
   * Intercepts calls to OpenRouter and Together.ai endpoints
   * and modifies reasoning content in streaming/non-streaming
   * responses for Deepseek models.
   * ------------------------------------------------------
   */
  window.fetch = async function(...args) {
    try {
      const [url, options] = args;
      const requestBody = options?.body;

      // 1. If not a target URL, fall back to the original fetch
      if (!isTargetURL(url)) {
        return originalFetch.apply(this, args);
      }

      // 2. If no request body, we can't parse or modify
      if (!requestBody) {
        return originalFetch.apply(this, args);
      }

      // 3. Attempt to parse the request body to identify the model
      let parsedBody;
      try {
        parsedBody = JSON.parse(requestBody);
      } catch {
        // If parse fails, just continue with the original fetch
        return originalFetch.apply(this, args);
      }

      // 4. If it's not a Deepseek model or it's a title request, do nothing special
      if (!isDeepseekModel(parsedBody) || isTitleRequest(parsedBody)) {
        return originalFetch.apply(this, args);
      }

      // 5. Make the actual fetch call to get the response
      const response = await originalFetch.apply(this, args);

      // 6. Check the content-type for streaming
      if (response.headers.get('content-type')?.includes('text/event-stream')) {
        // Use our special streaming handler
        return processStreamingResponse(response);
      }

      // 7. Otherwise, handle it as a normal JSON response
      return processNonStreamingResponse(response);
    } catch (error) {
      console.error('Error in fetch interceptor:', error);
      // If there's an error, fallback to the original fetch
      return originalFetch.apply(this, args);
    }
  };

  console.log('Deepseek reasoning extension loaded (with timing & updated emojis)!');
})();
