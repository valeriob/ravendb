﻿using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using Raven.Client;
using Sparrow;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents
{
    public sealed class BlittableMetadataModifier : IDisposable, IBlittableDocumentModifier
    {
        private bool _readingMetadataObject;
        private int _depth;
        private State _state = State.None;
        private bool _verifyStartArray;

        public BlittableMetadataModifier(JsonOperationContext context)
        {
            _ctx = context;
        }

        public LazyStringValue Id;
        public LazyStringValue ChangeVector;
        public DocumentFlags Flags;
        public NonPersistentDocumentFlags NonPersistentFlags;

        private JsonOperationContext _ctx;
        private LazyStringValue _metadataCollections;
        private LazyStringValue _metadataExpires;

        private readonly FastList<AllocatedMemoryData> _allocations = new FastList<AllocatedMemoryData>();

        private const string LegacyRevisionState = "Historical";
        private const string LegacyHasRevisionsDocumentState = "Current";

        private DocumentFlags ReadFlags(JsonParserState state)
        {
            var str = CreateLazyStringValueFromParserState(state);
            if (Enum.TryParse(str, true, out DocumentFlags flags) == false)
                ThrowInvalidFlagsProperty(str);
            return flags;
        }

        private unsafe LazyStringValue CreateLazyStringValueFromParserState(JsonParserState state)
        {
            int escapePositionsCount = state.EscapePositions.Count;

            var maxSizeOfEscapePos = escapePositionsCount * 5 // max size of var int
                                     + JsonParserState.VariableSizeIntSize(escapePositionsCount);

            var mem = _ctx.GetMemory(maxSizeOfEscapePos + state.StringSize);
            _allocations.Add(mem);
            Memory.Copy(mem.Address, state.StringBuffer, state.StringSize);
            var lazyStringValueFromParserState = _ctx.AllocateStringValue(null, mem.Address, state.StringSize);
            if (escapePositionsCount > 0)
            {
                lazyStringValueFromParserState.EscapePositions = state.EscapePositions.ToArray();
            }
            return lazyStringValueFromParserState;
        }

        private enum State
        {
            None,
            ReadingId,
            ReadingFlags,
            ReadingChangeVector,
            IgnoreProperty,
            IgnoreArray,
            IgnoreRevisionStatusProperty
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void StartObject()
        {
            if (_readingMetadataObject == false)
                return;

            _depth++;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void EndObject()
        {
            if (_readingMetadataObject == false)
                return;

            _depth--;

            Debug.Assert(_depth >= 0);
            if (_depth == 0)
                _readingMetadataObject = false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool AboutToReadPropertyName(IJsonParser reader, JsonParserState state)
        {
            if (reader is UnmanagedJsonParser)
                return AboutToReadPropertyNameInternal((UnmanagedJsonParser)reader, state);
            if (reader is ObjectJsonParser)
                return AboutToReadPropertyNameInternal((ObjectJsonParser)reader, state);

            return AboutToReadPropertyNameInternal(reader, state);
        }

        // TODO: When we hit CoreCLR 2.0 see if we can devirtualize this code with some generics shenanigans
        private unsafe bool AboutToReadPropertyNameInternal(UnmanagedJsonParser reader, JsonParserState state)
        {
            if (_state != State.None)
            {
                if (!AboutToReadWithStateUnlikely(reader, state))
                    return false;
            }

            _state = State.None;

            while (true)
            {
                if (reader.Read() == false)
                    return false;

                if (state.CurrentTokenType != JsonParserToken.String)
                    return true; // let the caller handle that

                if (_readingMetadataObject == false)
                {
                    if (state.StringSize == 9 && state.StringBuffer[0] == (byte)'@' && *(long*)(state.StringBuffer + 1) == 7022344802737087853)
                        _readingMetadataObject = true;

                    return true;
                }

                if (AboutToReadPropertyNameInMetadataUnlikely(reader, state, out bool aboutToReadPropertyName))
                    return aboutToReadPropertyName;
            }
        }

        // TODO: When we hit CoreCLR 2.0 see if we can devirtualize this code with some generics shenanigans
        private unsafe bool AboutToReadPropertyNameInternal(ObjectJsonParser reader, JsonParserState state)
        {
            if (_state != State.None)
            {
                if (!AboutToReadWithStateUnlikely(reader, state))
                    return false;
            }

            _state = State.None;

            while (true)
            {
                if (reader.Read() == false)
                    return false;

                if (state.CurrentTokenType != JsonParserToken.String)
                    return true; // let the caller handle that

                if (_readingMetadataObject == false)
                {
                    if (state.StringSize == 9 && state.StringBuffer[0] == (byte)'@' && *(long*)(state.StringBuffer + 1) == 7022344802737087853)
                        _readingMetadataObject = true;

                    return true;
                }

                if (AboutToReadPropertyNameInMetadataUnlikely(reader, state, out bool aboutToReadPropertyName))
                    return aboutToReadPropertyName;
            }
        }

        // TODO: When we hit CoreCLR 2.0 see if we can devirtualize this code with some generics shenanigans
        private unsafe bool AboutToReadPropertyNameInternal(IJsonParser reader, JsonParserState state)
        {
            if (_state != State.None)
            {
                if (!AboutToReadWithStateUnlikely(reader, state))
                    return false;
            }

            _state = State.None;

            while (true)
            {
                if (reader.Read() == false)
                    return false;

                if (state.CurrentTokenType != JsonParserToken.String)
                    return true; // let the caller handle that

                if (_readingMetadataObject == false)
                {
                    if (state.StringSize == 9 && state.StringBuffer[0] == (byte)'@' && *(long*)(state.StringBuffer + 1) == 7022344802737087853)
                        _readingMetadataObject = true;

                    return true;
                }

                if (AboutToReadPropertyNameInMetadataUnlikely(reader, state, out bool aboutToReadPropertyName))
                    return aboutToReadPropertyName;
            }
        }

        private unsafe bool AboutToReadPropertyNameInMetadataUnlikely(IJsonParser reader, JsonParserState state, out bool aboutToReadPropertyName)
        {
            aboutToReadPropertyName = true;

            switch (state.StringSize)
            {
                default: // accept this property
                    {
                        return true;
                    }

                case -1: // IgnoreProperty
                    {
                        if (reader.Read() == false)
                        {
                            _state = State.IgnoreProperty;
                            {
                                aboutToReadPropertyName = false;
                                return true;
                            }
                        }
                        if (state.CurrentTokenType == JsonParserToken.StartArray ||
                            state.CurrentTokenType == JsonParserToken.StartObject)
                            ThrowInvalidMetadataProperty(state);
                        break;
                    }

                case 3: // @id
                    if (state.StringBuffer[0] != (byte)'@' ||
                        *(short*)(state.StringBuffer + 1) != 25705)
                    {
                        aboutToReadPropertyName = true;
                        return true;
                    }

                    if (reader.Read() == false)
                    {
                        _state = State.ReadingId;
                        {
                            aboutToReadPropertyName = false;
                            return true;
                        }
                    }
                    if (state.CurrentTokenType != JsonParserToken.String)
                        ThrowExpectedFieldTypeOfString(Constants.Documents.Metadata.Id, state);
                    Id = CreateLazyStringValueFromParserState(state);
                    break;
                case 6: // @flags
                    if (state.StringBuffer[0] != (byte)'@' ||
                        *(int*)(state.StringBuffer + 1) != 1734437990 ||
                        state.StringBuffer[1 + sizeof(int)] != (byte)'s')
                    {
                        aboutToReadPropertyName = true;
                        return true;
                    }

                    if (reader.Read() == false)
                    {
                        _state = State.ReadingFlags;
                        {
                            aboutToReadPropertyName = false;
                            return true;
                        }
                    }
                    if (state.CurrentTokenType != JsonParserToken.String)
                        ThrowExpectedFieldTypeOfString(Constants.Documents.Metadata.Flags, state);
                    Flags = ReadFlags(state);
                    break;

                case 12: // @index-score
                    if (state.StringBuffer[0] != (byte)'@' ||
                        *(long*)(state.StringBuffer + 1) != 7166121427196997225 ||
                        *(short*)(state.StringBuffer + 1 + sizeof(long)) != 29295 ||
                        state.StringBuffer[1 + sizeof(long) + sizeof(short)] != (byte)'e')
                    {
                        aboutToReadPropertyName = true;
                        return true;
                    }

                    goto case -1;
                case 13: //Last-Modified
                    if (*(long*)state.StringBuffer != 7237087983830262092 ||
                        *(int*)(state.StringBuffer + sizeof(long)) != 1701406313 ||
                        state.StringBuffer[12] != (byte)'d')
                    {
                        aboutToReadPropertyName = true;
                        return true;
                    }

                    goto case -1;

                case 14:
                    if (state.StringBuffer[0] == (byte)'@')
                    {
                        // @change-vector
                        if (*(long*)(state.StringBuffer + 1) == 8515573965335390307 &&
                            *(int*)(state.StringBuffer + 1 + sizeof(long)) == 1869898597 &&
                            state.StringBuffer[1 + sizeof(long) + sizeof(int)] == (byte)'r')
                        {
                            if (reader.Read() == false)
                            {
                                _state = State.ReadingChangeVector;
                                {
                                    aboutToReadPropertyName = false;
                                    return true;
                                }
                            }
                            if (state.CurrentTokenType != JsonParserToken.String)
                                ThrowExpectedFieldTypeOfString(Constants.Documents.Metadata.ChangeVector, state);
                            ChangeVector = CreateLazyStringValueFromParserState(state);
                            break;
                        }

                        // @last-modified
                        if (*(long*)(state.StringBuffer + 1) == 7237123168202350956 &&
                            *(int*)(state.StringBuffer + 1 + sizeof(long)) == 1701406313 &&
                            state.StringBuffer[1 + sizeof(long) + sizeof(int)] == (byte)'d')
                        {
                            goto case -1;
                        }
                    }

                    {
                        aboutToReadPropertyName = true;
                        return true;
                    }
                case 15: //Raven-Read-Only
                    if (*(long*)state.StringBuffer != 7300947898092904786 ||
                        *(int*)(state.StringBuffer + sizeof(long)) != 1328374881 ||
                        *(short*)(state.StringBuffer + sizeof(long) + sizeof(int)) != 27758 ||
                        state.StringBuffer[14] != (byte)'y')
                    {
                        aboutToReadPropertyName = true;
                        return true;
                    }

                    goto case -1;

                case 17: //Raven-Entity-Name --> @collection
                    if (*(long*)state.StringBuffer != 7945807069737017682 ||
                        *(long*)(state.StringBuffer + sizeof(long)) != 7881666780093245812 ||
                        state.StringBuffer[16] != (byte)'e')
                    {
                        aboutToReadPropertyName = true;
                        return true;
                    }

                    var collection = _metadataCollections;
                    state.StringBuffer = collection.AllocatedMemoryData.Address;
                    state.StringSize = collection.Size;
                    {
                        aboutToReadPropertyName = true;
                        return true;
                    }
                case 19: //Raven-Last-Modified
                    if (*(long*)state.StringBuffer != 7011028672080929106 ||
                        *(long*)(state.StringBuffer + sizeof(long)) != 7379539893622240371 ||
                        *(short*)(state.StringBuffer + sizeof(long) + sizeof(long)) != 25961 ||
                        state.StringBuffer[18] != (byte)'d')
                    {
                        aboutToReadPropertyName = true;
                        return true;
                    }

                    goto case -1;
                case 21: //Raven-Expiration-Date
                    if (*(long*)state.StringBuffer != 8666383010116297042 ||
                        *(long*)(state.StringBuffer + sizeof(long)) != 7957695015158966640 ||
                        *(short*)(state.StringBuffer + sizeof(long) + sizeof(long)) != 17453 ||
                        state.StringBuffer[20] != (byte)'e')
                    {
                        aboutToReadPropertyName = true;
                        return true;
                    }

                    var expires = _metadataExpires;
                    state.StringBuffer = expires.AllocatedMemoryData.Address;
                    state.StringSize = expires.Size;
                    {
                        aboutToReadPropertyName = true;
                        return true;
                    }
                case 23: //Raven-Document-Revision
                    if (*(long*)state.StringBuffer != 8017583188798234962 ||
                        *(long*)(state.StringBuffer + sizeof(long)) != 5921517102558967139 ||
                        *(int*)(state.StringBuffer + sizeof(long) + sizeof(long)) != 1936291429 ||
                        *(short*)(state.StringBuffer + sizeof(long) + sizeof(long) + sizeof(int)) != 28521 ||
                        state.StringBuffer[22] != (byte)'n')
                    {
                        aboutToReadPropertyName = true;
                        return true;
                    }

                    goto case -1;
                case 24: //Raven-Replication-Source
                    if (*(long*)state.StringBuffer != 7300947898092904786 ||
                        *(long*)(state.StringBuffer + sizeof(long)) != 8028075772393122928 ||
                        *(long*)(state.StringBuffer + sizeof(long) + sizeof(long)) != 7305808869229538670)
                    {
                        aboutToReadPropertyName = true;
                        return true;
                    }

                    goto case -1;
                case 25: //Raven-Replication-Version OR Raven-Replication-History
                    if (*(long*)state.StringBuffer != 7300947898092904786 ||
                        *(long*)(state.StringBuffer + sizeof(long)) != 8028075772393122928)
                    {
                        aboutToReadPropertyName = true;
                        return true;
                    }

                    var value = *(long*)(state.StringBuffer + sizeof(long) + sizeof(long));
                    var lastByte = state.StringBuffer[24];
                    if ((value != 8028074745928232302 || lastByte != (byte)'n') &&
                        (value != 8245937481775066478 || lastByte != (byte)'y'))
                    {
                        aboutToReadPropertyName = true;
                        return true;
                    }

                    var isReplicationHistory = lastByte == (byte)'y';
                    if (reader.Read() == false)
                    {
                        _verifyStartArray = isReplicationHistory;
                        _state = isReplicationHistory ? State.IgnoreArray : State.IgnoreProperty;
                        {
                            aboutToReadPropertyName = false;
                            return true;
                        }
                    }

                    // Raven-Replication-History is an array
                    if (isReplicationHistory)
                    {
                        if (state.CurrentTokenType != JsonParserToken.StartArray)
                            ThrowInvalidReplicationHistoryType(state);

                        do
                        {
                            if (reader.Read() == false)
                            {
                                _state = State.IgnoreArray;
                                {
                                    aboutToReadPropertyName = false;
                                    return true;
                                }
                            }
                        } while (state.CurrentTokenType != JsonParserToken.EndArray);
                    }
                    else if (state.CurrentTokenType == JsonParserToken.StartArray ||
                             state.CurrentTokenType == JsonParserToken.StartObject)
                        ThrowInvalidMetadataProperty(state);
                    break;
                case 29: //Non-Authoritative-Information
                    if (*(long*)state.StringBuffer != 7526769800038477646 ||
                        *(long*)(state.StringBuffer + sizeof(long)) != 8532478930943832687 ||
                        *(long*)(state.StringBuffer + sizeof(long) + sizeof(long)) != 7886488383206796645 ||
                        *(int*)(state.StringBuffer + sizeof(long) + sizeof(long) + sizeof(long)) != 1869182049 ||
                        state.StringBuffer[28] != (byte)'n')
                    {
                        aboutToReadPropertyName = true;
                        return true;
                    }

                    goto case -1;

                case 30: //Raven-Document-Parent-Revision OR Raven-Document-Revision-Status
                    if (*(long*)state.StringBuffer != 8017583188798234962)
                    {
                        aboutToReadPropertyName = true;
                        return true;
                    }

                    if ((*(long*)(state.StringBuffer + sizeof(long)) != 5777401914483111267 ||
                         *(long*)(state.StringBuffer + sizeof(long) + sizeof(long)) != 7300947924012593761 ||
                         *(int*)(state.StringBuffer + sizeof(long) + sizeof(long) + sizeof(long)) != 1769171318 ||
                         *(short*)(state.StringBuffer + sizeof(long) + sizeof(long) + sizeof(long) + sizeof(int)) != 28271) &&
                        (*(long*)(state.StringBuffer + sizeof(long)) != 5921517102558967139 ||
                         *(long*)(state.StringBuffer + sizeof(long) + sizeof(long)) != 3273676477843469925 ||
                         *(int*)(state.StringBuffer + sizeof(long) + sizeof(long) + sizeof(long)) != 1952543827 ||
                         *(short*)(state.StringBuffer + sizeof(long) + sizeof(long) + sizeof(long) + sizeof(int)) != 29557))
                    {
                        aboutToReadPropertyName = true;
                        return true;
                    }

                    var isRevisionStatusProperty = state.StringBuffer[29] == 's';
                    if (reader.Read() == false)
                    {
                        _state = isRevisionStatusProperty ? State.IgnoreRevisionStatusProperty : State.IgnoreProperty;
                        {
                            aboutToReadPropertyName = false;
                            return true;
                        }
                    }

                    if (state.CurrentTokenType == JsonParserToken.StartArray ||
                        state.CurrentTokenType == JsonParserToken.StartObject)
                        ThrowInvalidMetadataProperty(state);

                    if (isRevisionStatusProperty)
                    {
                        switch (CreateLazyStringValueFromParserState(state))
                        {
                            case LegacyHasRevisionsDocumentState:
                                NonPersistentFlags |= NonPersistentDocumentFlags.LegacyHasRevisions;
                                break;
                            case LegacyRevisionState:
                                NonPersistentFlags |= NonPersistentDocumentFlags.LegacyRevision;
                                break;
                        }
                    }

                    break;
                case 32: //Raven-Replication-Merged-History
                    if (*(long*)state.StringBuffer != 7300947898092904786 ||
                        *(long*)(state.StringBuffer + sizeof(long)) != 8028075772393122928 ||
                        *(long*)(state.StringBuffer + sizeof(long) + sizeof(long)) != 7234302117464059246 ||
                        *(long*)(state.StringBuffer + sizeof(long) + sizeof(long) + sizeof(long)) != 8751179571877464109)
                    {
                        aboutToReadPropertyName = true;
                        return true;
                    }

                    goto case -1;
            }
            return false;
        }

        private bool AboutToReadWithStateUnlikely(IJsonParser reader, JsonParserState state)
        {
            switch (_state)
            {
                case State.None:
                    break;
                case State.IgnoreProperty:
                    if (reader.Read() == false)
                        return false;

                    if (state.CurrentTokenType == JsonParserToken.StartArray ||
                        state.CurrentTokenType == JsonParserToken.StartObject)
                        ThrowInvalidMetadataProperty(state);
                    break;
                case State.IgnoreArray:
                    if (_verifyStartArray)
                    {
                        if (reader.Read() == false)
                            return false;

                        _verifyStartArray = false;

                        if (state.CurrentTokenType != JsonParserToken.StartArray)
                            ThrowInvalidReplicationHistoryType(state);
                    }
                    while (state.CurrentTokenType != JsonParserToken.EndArray)
                    {
                        if (reader.Read() == false)
                            return false;
                    }
                    break;
                case State.IgnoreRevisionStatusProperty:
                    if (reader.Read() == false)
                        return false;

                    if (state.CurrentTokenType != JsonParserToken.String &&
                        state.CurrentTokenType != JsonParserToken.Integer)
                        ThrowInvalidEtagType(state);

                    switch (CreateLazyStringValueFromParserState(state))
                    {
                        case LegacyHasRevisionsDocumentState:
                            NonPersistentFlags |= NonPersistentDocumentFlags.LegacyHasRevisions;
                            break;
                        case LegacyRevisionState:
                            NonPersistentFlags |= NonPersistentDocumentFlags.LegacyRevision;
                            break;
                    }
                    break;
                case State.ReadingId:
                    if (reader.Read() == false)
                        return false;

                    if (state.CurrentTokenType != JsonParserToken.String)
                        ThrowExpectedFieldTypeOfString(Constants.Documents.Metadata.Id, state);
                    Id = CreateLazyStringValueFromParserState(state);
                    break;
                case State.ReadingFlags:
                    if (reader.Read() == false)
                        return false;

                    if (state.CurrentTokenType != JsonParserToken.String)
                        ThrowExpectedFieldTypeOfString(Constants.Documents.Metadata.Flags, state);
                    Flags = ReadFlags(state);
                    break;
                case State.ReadingChangeVector:
                    if (reader.Read() == false)
                        return false;

                    if (state.CurrentTokenType != JsonParserToken.String)
                        ThrowExpectedFieldTypeOfString(Constants.Documents.Metadata.ChangeVector, state);
                    ChangeVector = CreateLazyStringValueFromParserState(state);

                    break;
            }
            return true;
        }

        private static void ThrowInvalidMetadataProperty(JsonParserState state)
        {
            throw new InvalidDataException($"Expected property @metadata to be a simpel type, but was {state.CurrentTokenType}");
        }

        private static void ThrowExpectedFieldTypeOfString(string field, JsonParserState state)
        {
            throw new InvalidDataException($"Expected property @metadata.{field} to have string type, but was: {state.CurrentTokenType}");
        }

        private static void ThrowInvalidFlagsProperty(LazyStringValue str)
        {
            throw new InvalidDataException($"Cannot parse the value of property @metadata.@flags: {str}");
        }

        private static void ThrowInvalidEtagType(JsonParserState state)
        {
            throw new InvalidDataException($"Expected property @metadata.@etag to have string or long type, but was: {state.CurrentTokenType}");
        }

        private static void ThrowInvalidReplicationHistoryType(JsonParserState state)
        {
            throw new InvalidDataException($"Expected property @metadata.Raven-Replication-History to have array type, but was: {state.CurrentTokenType}");
        }

        public void Dispose()
        {
            for (int i = _allocations.Count - 1; i >= 0; i--)
            {
                _ctx.ReturnMemory(_allocations[i]);
            }
            _allocations.Clear();
        }

        public void Reset(JsonOperationContext ctx)
        {
            if (_ctx == null) // should never happen
            {
                _ctx = ctx;
                _metadataCollections = _ctx.GetLazyStringForFieldWithCaching(CollectionName.MetadataCollectionSegment);
                _metadataExpires = _ctx.GetLazyStringForFieldWithCaching(Constants.Documents.Metadata.Expires);
                return;
            }
            Id = null;
            ChangeVector = null;
            Flags = DocumentFlags.None;
            NonPersistentFlags = NonPersistentDocumentFlags.None;
            _depth = 0;
            _state = State.None;
            _readingMetadataObject = false;
            _ctx = ctx;
            _metadataCollections = _ctx.GetLazyStringForFieldWithCaching(CollectionName.MetadataCollectionSegment);
            _metadataExpires = _ctx.GetLazyStringForFieldWithCaching(Constants.Documents.Metadata.Expires);
        }
    }
}