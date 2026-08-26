# ah4c

## The tune is the product

This proxy exists to hand Channels DVR a video stream. Everything else here —
captions, the speech models, the graphics driver, the web pages — is decoration
on top of that, and decoration never costs a recording.

**The DVR gives up after 30 seconds.** From the request arriving to the first
bytes reaching it. That is the entire budget and it is not negotiable, because
it belongs to the DVR and not to us. A tune that takes 31 seconds is a failed
recording, and a failed recording is the worst thing this program can do.

Before adding anything, answer: can this run while a tune is in flight? If the
answer is anything but a confident no, it must not run then.

### Rules that have been learned the hard way

Each of these cost a recording before it was written down.

1. **Never start uninterruptible work without proven quiet.** Not "the machine
   looks quiet right now" — a container that has just come up is quiet because
   the DVR has not asked yet, and the storm is two seconds away. Use
   `waitTuneQuietHeld`, which requires quiet to be *held*.

2. **Never override the quiet gate on a timer.** "It has waited long enough, go
   ahead anyway" is how the driver install killed a tune after the gate had
   correctly held it off for forty seconds. If quiet never comes, the work
   waits — possibly for ever. That is the correct outcome.

3. **`nice` and `ionice` are not permission to proceed.** They help; they do not
   make heavy I/O safe. An array does not care what priority the process
   writing to it has. They are a backstop for being wrong, not a license.

4. **Long work must yield throughout, not just at the start.** A gate at the
   door of a job that runs for minutes protects nothing: the world changes
   while it runs. Downloads pause per read (`tunesPending()`); model loads
   re-check.

5. **Work that cannot be paused can usually be divided.** This was written as
   "anything that cannot be paused must not be started", and that let a
   thirty-second package install through on ten seconds of proven quiet — a
   gate can only promise the moment it is asked, and a DVR starting several
   recordings at once arrives in the middle. dpkg cannot be interrupted, but
   thirty-seven packages is thirty-seven jobs of a second or two with the gate
   re-checked between each. Before accepting that something must run
   uninterrupted, ask whether it is really one job or many.

6. **Nothing on the tune path may block.** `maybeWrapCaptions` and everything it
   calls must be effectively free. Loading, probing, dlopening and downloading
   all happen on other goroutines, after the stream is already flowing.

7. **If a gate can starve something, separate the two concerns.** The engine's
   open waited on the driver restore, so an unbounded wait stopped captions
   entirely. The answer is not to bound the gate — it is to release the waiter
   and let the gated work keep waiting.

8. **Every gate is bounded and every gate has a voice.** `for
   !waitTuneQuietHeld(...) {}` waits for a stretch of quiet a three-tuner
   machine does not reliably produce, and says nothing while it waits — so the
   driver never installed, captions never started, and from outside it looked
   like a build that had stopped working. A gate that expires is a decision the
   log has to be able to explain afterwards; that is what `awaitQuiet` is for.
   Rule 2 forbids overriding a gate on a timer and still does: a gate may expire
   and proceed only when the work behind it has already been made divided or
   polite, never as a way of getting an un-pausable job started.

9. **When a gate says no, the unit of work waits — it is never skipped.** The
   per-package install wrote `i--; continue` inside a `for i, deb := range`,
   where the next iteration assigns `i` from the range anyway. The decrement did
   nothing and the package was dropped instead, quietly, on exactly the busy
   machines the gate exists for. A driver missing most of its libraries installs
   cleanly, loads cleanly and offers no device: not a failure, a silent partial
   success, which is the hardest kind to see. Deferring and dropping look
   identical in a loop and are opposites in effect.

10. **Guaranteed quiet beats proven quiet, and startup is the only guaranteed
    quiet there is.** Everything above is refereeing a race between the driver
    install and the tunes. `main` calls `restoreGPURuntime` before it binds
    7654, and until that port is bound the DVR cannot ask for anything — it gets
    connection refused, not a request nobody answers. There is no tune to
    interrupt, so there is nothing to gate. Rules 1 through 9 are what to do when
    the race cannot be removed; ask first whether it can be. If a piece of work
    has to happen once per container and cannot be paused, in front of the
    listener is where it belongs, bounded so a container that cannot do it still
    comes up.

11. **A thing that looks like a bug may be the thing that works.** The hold
    opened the encoder with `http.Client{Timeout: 20 * time.Second}`. That field
    covers reading the body, not just connecting, so twenty seconds into a held
    tune the encoder's body was force-closed mid-stream — which reads exactly
    like a mistake, and was removed as one. It was load-bearing. Breaking the
    connection is what discards whatever the DVR has stored ahead of the show,
    because reopening starts again at the encoder's live output. It is the only
    thing here that *sheds* lag: the DVR's buffer is downstream and nothing in
    this program can take bytes out of it. Removing the timeout removed the only
    thing pulling the viewer back to the live edge, and the tune went seconds
    behind and stayed there. Three builds: with it, at the live edge; without
    it, behind; with it restored, at the live edge again.

    It is not a timeout any more. A break by timeout happens at a moment nothing
    chose and leaves nothing able to decline it — the connection is closed by
    force and then has to be opened again, and an encoder that will not take a
    second reader while a tuner owns the stream has no way to say so. So
    `refreshSource` makes the break instead of suffering it: the new connection
    is opened first, and only once it is up is the old one closed. An encoder
    that refuses costs nothing, and is not asked twice.

    Say what is known and what is inferred. It happens **once** per tune, not
    every twenty seconds — the reopened connection has no body timeout, so after
    one break the stream runs on. Nobody has yet watched a recording across that
    mark. That removing the break puts the viewer behind is measured; that a
    discarded buffer is *why* is the best explanation available, not a proven
    one.

    Two things follow generally. Before deleting something that looks wrong,
    find out what goes quiet when it is gone — here it was `encoder stream ended
    …; reconnecting`, and that line disappearing *was* the regression. And a log
    going silent is a symptom, not the absence of one: a reader that has stopped
    produces no errors because nothing is flowing through it.

12. **The ffmpeg in the image is not the ffmpeg on your machine.** A PNG
    pre-roll never appeared. Every test of it passed here, because a Homebrew
    ffmpeg decodes everything. The one in the image is Channels' build, meant
    for transcoding video, and among still formats it decodes mjpeg and nothing
    else — `Decoder (codec png) not found`. It has no `setsar` filter either,
    which was separately breaking JPEG stills. A still is now decoded with Go's
    own decoders and handed over as a JPEG, so nothing is asked of ffmpeg that
    it cannot do.

    The image is right there: `docker run --rm --entrypoint /bin/sh <image> -c
    'ffmpeg -decoders'`. Two minutes of that would have saved an evening.
    Anything that shells out — ffmpeg, ffprobe, adb — gets checked in the
    container before it is believed.

13. **Reasoning is not evidence, and a log line beats an argument.** In one
    evening these all shipped and all were wrong: rewriting the program's
    tables so the player would re-read them; making a discontinuity marker
    actually mark, on the one path that was working *because* it was inert;
    moving the caption engine to start at the hand-off, which put its whole
    start-up cost between dropping the encoder's queue and the first frame;
    and inventing a service for the hold to advertise. Every one was coherent,
    tested, and aimed at something that was not the problem.

    What found each real cause was a measurement or a line of the user's log —
    `Client.Timeout ... while reading body`, `Decoder (codec png) not found`,
    `start on keyframe, 1.629s from the mark`. Before changing behaviour, have
    a number or a log line that says what is wrong. "This should help" is how
    a working build gets broken.

    Two corollaries. Change one thing between comparisons: forty-five seconds
    was tested on one build and sixty on another, and hours went into
    explaining a difference that may not have existed. And if something looks
    inert on a path that works, leave it — see rule 11.

## Working here

- **`main.go` is being broken up, not grown.** New behavior goes in its own
  file, named for the feature or the environment variable that switches it on
  (`nullframeinsertion.go`, `nullframedelay.go`, `preroll.go`, the caption and
  model files), and the edit to `main.go` is the wiring: a call at the place
  the feature hooks in, an `[ENV]` line, and nothing else. When something in
  `main.go` needs more than that, move it out into a file first and change it
  there. The playback gate there is 40s plus an 8s keyframe wait against the
  DVR's 30, which is a real problem; `NULL_FRAME_DELAY` is the answer to it
  from outside the gate, so fix the contention rather than the gate.
- **American spelling everywhere.** Code, comments, commit messages, log lines,
  user-facing text. See `~/.claude/CLAUDE.md`.
- **Each speech model owns a file.** `cohere.go`, `nemotron.go`, `moonshine.go`
  hold the catalog entry and whatever that model asks of the code around it.
  `captions.go` is the common part and knows nothing about which model it is
  serving. A model is attached in exactly two places — `captionModelCatalog` and
  `quirksFor` — so deleting its file breaks three lines the compiler names.
- **Go to the model's or the engine's documentation before tuning a number.**
  Phrase lengths, quantization, KV precision, language codes and hallucination
  behavior are all documented, and every one of them has been guessed wrong
  here at least once. A number invented and given a confident justification is
  worse than an obvious placeholder.
- **A rule that decides whether to discard audio or text gets its own function
  and a test.** Two of them have been silently wrong — one because an edit never
  saved and the mistake was only visible in a commit message. Conditions buried
  in loops cannot be checked.
- **What bounds a hold is how long the DVR is left starving, not how long the
  hold is.** After the detection window the filler drops to one packet every
  half second — three kilobits — and a DVR sits through about twenty seconds of
  that and then decides the stream has died. A forty-five second hold leaves it
  there for twenty-one seconds and works; a sixty second hold leaves it there
  for thirty-six and the DVR tunes again. So the volume comes back for a second
  every five, and the thin stretch is four seconds whatever the hold's length.
  `PLAYBACK_DELAY` is still capped at `holdMost`, and the reason is now
  measured rather than guessed: **the DVR's timeline starts when the response
  headers land and advances only when media arrives.** Its log says `Opened
  connection` at request plus 18.9 seconds on every held tune — the end of the
  1xx window — and NULL packets carry no time, so every second of hold after
  that is a second the viewer is behind, for the whole session. A forty-five
  second hold is at the live edge; ninety is far behind it. Reopening the
  encoder five seconds into the program — a timestamp jump — changed nothing,
  because a DVR stitches over discontinuities rather than resetting its clock.
  Nothing sent after a hold can put those seconds back. Only the pre-roll
  moves the clock during the wait, because it is real frames, which is also
  why it lands in the recording; a longer hold has to be one or the other.
  (The log that started the starvation theory does not match it either: the
  sixty second run that "tuned again at thirty-one seconds" was the client
  hanging up 2.8 seconds after the headers, inside the volume window, and its
  retry ran through. The heartbeat is harmless. It was not the fix.)

  The explanation that got there first was that a hold carries no *service* —
  only PID 0x1FFF, no PAT, no PMT — so the DVR gives up for want of a program.
  A version of this built and shipped tables naming PIDs nothing was ever sent
  on, and the DVR locked onto them and never played at all. It is wrong twice
  over: a forty-five second hold carries no program either and forty-five
  works, and tables invented here cannot name what the encoder is about to
  send. Do not rebuild it.

- **Say what was measured and what was assumed.** The log prints the real-time
  factor, the backend actually chosen, and the numbers behind anything held
  back, so a disagreement can be settled with evidence rather than opinion.

## Branches

`main` and `ui-refactor` are byte-identical apart from `Dockerfile` and
`.github/workflows/build.yml`, deliberately. Caption work is committed to `main`
and cherry-picked to `ui-refactor`; both get pushed. Do not sync those two files
and do not build images off `ui-refactor` — it is the upstream pull request.
