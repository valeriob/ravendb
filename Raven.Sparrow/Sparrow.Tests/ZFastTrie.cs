﻿using Sparrow.Binary;
using Sparrow.Collections;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Extensions;

namespace Sparrow.Tests
{
    public class ZFastTrieTest
    {
        private readonly Func<string, BitVector> binarize = x => BitVector.Of(x, true);


        [Fact]
        public void Construction()
        {
            var tree = new ZFastTrieSortedSet<string, string>(binarize);
            Assert.Equal(0, tree.Count);
            Assert.Null(tree.FirstKeyOrDefault());
            Assert.Null(tree.LastKeyOrDefault());

            ZFastTrieDebugHelpers.StructuralVerify(tree);
        }


        [Fact]
        public void Operations_SingleElement()
        {
            var key = "oren";

            var tree = new ZFastTrieSortedSet<string, string>(binarize);
            Assert.True(tree.Add(key, "eini"));
            Assert.Equal(key, tree.FirstKey());
            Assert.Equal(key, tree.LastKey());
            Assert.True(tree.Contains(key));

            string value;
            Assert.True(tree.TryGet(key, out value));

            // x+ = min{y ∈ S | y ≥ x} (the successor of x in S) - Page 160 of [1]
            // Therefore the successor of the key "oren" is greater or equal to "oren"
            Assert.Equal(key, tree.SuccessorOrDefault(key));
            Assert.Null(tree.SuccessorOrDefault("qu"));

            // x− = max{y ∈ S | y < x} (the predecessor of x in S) - Page 160 of [1] 
            // Therefore the predecessor of the key "oren" is strictly less than "oren".
            Assert.Null(tree.PredecessorOrDefault(key));
            Assert.Null(tree.PredecessorOrDefault("aq"));
            Assert.Equal(key, tree.PredecessorOrDefault("pq"));

            ZFastTrieDebugHelpers.StructuralVerify(tree);
        }

        [Fact]
        public void Structure_SingleElement()
        {
            var key = "oren";

            var tree = new ZFastTrieSortedSet<string, string>(binarize);
            Assert.True(tree.Add(key, "eini"));

            var successor = tree.SuccessorInternal(key);
            Assert.True(successor.IsLeaf);
            Assert.Null(successor.Next.Key);
            Assert.Null(successor.Previous.Key);
            Assert.Equal(tree.Head, successor.Previous);
            Assert.Equal(tree.Tail, successor.Next);

            Assert.Equal(key, successor.Key);

            var predecessor = tree.PredecessorInternal("yy");
            Assert.True(predecessor.IsLeaf);
            Assert.Null(predecessor.Next.Key);
            Assert.Equal(tree.Head, predecessor.Previous);
            Assert.Equal(tree.Tail, predecessor.Next);
            Assert.Null(predecessor.Previous.Key);
            Assert.Equal(key, predecessor.Key);
                        
            Assert.Equal(predecessor, successor);
            Assert.Equal(tree.Root, predecessor);

            ZFastTrieDebugHelpers.StructuralVerify(tree);
        }

        [Fact]
        public void Operations_SingleBranchInsertion()
        {
            string smallestKey = "Ar";
            string lesserKey = "Oren";
            string greaterKey = "oren";
            string greatestKey = "zz";

            var tree = new ZFastTrieSortedSet<string, string>(binarize);
            Assert.True(tree.Add(lesserKey, "eini"));
            Assert.True(tree.Add(greaterKey, "Eini"));

            Assert.Equal(lesserKey, tree.FirstKey());
            Assert.Equal(greaterKey, tree.LastKey());

            Assert.True(tree.Contains(greaterKey));
            Assert.True(tree.Contains(lesserKey));

            string value;
            Assert.True(tree.TryGet(lesserKey, out value));
            Assert.True(tree.TryGet(greaterKey, out value));
            Assert.False(tree.TryGet(greaterKey + "1", out value));
            Assert.False(tree.TryGet("1", out value));

            // x+ = min{y ∈ S | y ≥ x} (the successor of x in S) - Page 160 of [1]
            // Therefore the successor of the key "oren" is greater or equal to "oren"
            Assert.Equal(lesserKey, tree.SuccessorOrDefault(lesserKey));
            Assert.Equal(greaterKey, tree.SuccessorOrDefault(greaterKey));
            Assert.Equal(greaterKey, tree.SuccessorOrDefault(lesserKey + "1"));
            Assert.Null(tree.SuccessorOrDefault(greatestKey));

            // x− = max{y ∈ S | y < x} (the predecessor of x in S) - Page 160 of [1] 
            // Therefore the predecessor of the key "oren" is strictly less than "oren".
            Assert.Equal(lesserKey, tree.PredecessorOrDefault(greaterKey));
            Assert.Null(tree.PredecessorOrDefault(lesserKey));
            Assert.Null(tree.PredecessorOrDefault(smallestKey));

            ZFastTrieDebugHelpers.StructuralVerify(tree);
        }

        [Fact]
        public void Structure_SingleBranchInsertion()
        {
            string lesserKey = "Oren";
            string midKey = "aa";
            string greaterKey = "oren";

            var tree = new ZFastTrieSortedSet<string, string>(binarize);
            Assert.True(tree.Add(lesserKey, "eini"));
            Assert.True(tree.Add(greaterKey, "Eini"));

            Assert.True(tree.Root.IsInternal);

            var successor = tree.SuccessorInternal(midKey);
            Assert.True(successor.IsLeaf);
            Assert.Null(successor.Next.Key);
            Assert.NotNull(successor.Previous.Key);
            Assert.Equal(tree.Tail, successor.Next);

            var predecessor = tree.PredecessorInternal(midKey);
            Assert.True(predecessor.IsLeaf);
            Assert.NotNull(predecessor.Next.Key);
            Assert.Equal(tree.Head, predecessor.Previous);
            Assert.Null(predecessor.Previous.Key);

            Assert.Equal(predecessor.Next, successor);
            Assert.Equal(successor.Previous, predecessor);

            ZFastTrieDebugHelpers.StructuralVerify(tree);
        }

        [Fact]
        public void Structure_MultipleBranchInsertion()
        {
            var tree = new ZFastTrieSortedSet<string, string>(binarize);

            Assert.True(tree.Add("8Jp3","8Jp3"));
            Assert.True(tree.Add("GX37", "GX37"));
            Assert.True(tree.Add("f04o", "f04o"));
            Assert.True(tree.Add("KmGx","KmGx"));

            ZFastTrieDebugHelpers.StructuralVerify(tree);
            ZFastTrieDebugHelpers.DumpKeys(tree);            
        }

        private static readonly string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

        private static string GenerateRandomString(Random generator, int size)
        {           
            var stringChars = new char[size];         
            for (int i = 0; i < stringChars.Length; i++)
                stringChars[i] = chars[generator.Next(chars.Length)];

            return new String(stringChars);
        }


        public static IEnumerable<object[]> TreeSize
        {
            get
            {
                // Or this could read from a file. :)
                return new[]
                {
                    new object[] { 102, 4, 4 },                    
                    //new object[] { 100, 4, 8 },
                    //new object[] { 101, 4, 16 },
                    //new object[] { 100, 8, 32 },
                    //new object[] { 100, 16, 256 }
                };
            }
        }

        [Theory, PropertyData("TreeSize")]
        public void Structure_CappedSizeInsertion( int seed, int size, int count )
        {
            var generator = new Random(seed);
 
            var tree = new ZFastTrieSortedSet<string, string>(binarize);

            Console.WriteLine("Insert order");
            for (int i = 0; i < count; i++)
            {
                string key = GenerateRandomString(generator, size);

                if (!tree.Contains(key))
                    tree.Add(key, key);

                Console.WriteLine(key);
            }

            ZFastTrieDebugHelpers.DumpKeys(tree);

            ZFastTrieDebugHelpers.StructuralVerify(tree);
        }
    }

    public static class ZFastTrieDebugHelpers
    {

        public static void DumpKeys<T, W>(ZFastTrieSortedSet<T, W> tree) where T : IEquatable<T>
        {
            Console.WriteLine("Tree stored order");

            var current = tree.Head.Next;
            while (current != null && current != tree.Tail)
            {
                Console.WriteLine(current.Key.ToString());
                current = current.Next;
            }
        }

        public static void DumpTree<T, W>(ZFastTrieSortedSet<T, W> tree) where T : IEquatable<T>
        {
            if (tree.Count == 0)
            {
                Console.WriteLine("Tree is empty.");
            }
            else
            {
                DumpNodes(tree, tree.Root, null, 0, 0);
            }
        }

        private static int DumpNodes<T, W>(ZFastTrieSortedSet<T, W> tree, ZFastTrieSortedSet<T, W>.Node node, ZFastTrieSortedSet<T, W>.Node parent, int nameLength, int depth) where T : IEquatable<T>
        {
            if (node == null)
                return 0;

            for (int i = depth; i-- != 0; )
                Console.Write('\t');

            if (node is ZFastTrieSortedSet<T, W>.Internal)
            {
                var internalNode = node as ZFastTrieSortedSet<T, W>.Internal;

                Console.WriteLine(string.Format("Node {0} (name length: {1}) Jump left: {2} Jump right: {3}", node.ToDebugString(tree), nameLength, internalNode.JumpLeftPtr.ToDebugString(tree), internalNode.JumpRightPtr.ToDebugString(tree)));

                return 1 + DumpNodes(tree, internalNode.Left, internalNode, internalNode.ExtentLength + 1, depth + 1)
                         + DumpNodes(tree, internalNode.Right, internalNode, internalNode.ExtentLength + 1, depth + 1);
            }
            else
            {
                Console.WriteLine(string.Format("Node {0} (name length: {1})", node.ToDebugString(tree), nameLength));

                return 1;
            }
        }


        public static void StructuralVerify<T, W>(ZFastTrieSortedSet<T, W> tree) where T : IEquatable<T>
        {
            Assert.NotNull(tree.Head);
            Assert.NotNull(tree.Tail);
            Assert.Null(tree.Tail.Next);
            Assert.Null(tree.Head.Previous);

            Assert.True(tree.Root == null || tree.Root.NameLength == 0); // Either the root does not exist or the root is internal and have name length == 0
            Assert.True(tree.Count == 0 && tree.NodesTable.Count == 0 || tree.Count == tree.NodesTable.SelectMany(x => x.Value).Count() + 1); 

            if (tree.Count == 0)
            {
                Assert.Equal(tree.Head, tree.Tail.Previous);
                Assert.Equal(tree.Tail, tree.Head.Next);

                Assert.NotNull(tree.NodesTable);
                Assert.Equal(0, tree.NodesTable.Count);

                return; // No more to check for an empty trie.
            }

            var root = tree.Root;
            var nodes = new HashSet<ZFastTrieSortedSet<T, W>.Node>();

            foreach (var nodesList in tree.NodesTable)
            {
                foreach (var node in nodesList.Value)
                {
                    int handleLength = node.GetHandleLength(tree);

                    Assert.True(root == node || root.GetHandleLength(tree) < handleLength); // All handled of lower nodes must be bigger than the root.
                    Assert.Equal(node, node.ReferencePtr.ReferencePtr); // The reference of the reference should be itself.

                    nodes.Add(node);
                }
            }

            Assert.Equal(tree.NodesTable.SelectMany(x => x.Value).Count(), nodes.Count); // We are ensuring there are no repeated nodes in the hash table. 

            if (tree.Count == 1)
            {
                Assert.Equal(tree.Root, tree.Head.Next);
                Assert.Equal(tree.Root, tree.Tail.Previous);
            }
            else
            {
                var toRight = tree.Head.Next;
                var toLeft = tree.Tail.Previous;

                for (int i = 1; i < tree.Count; i++)
                {
                    // Ensure there is name order in the linked list of leaves.
                    Assert.True(toRight.Name(tree).CompareTo(toRight.Next.Name(tree)) < 0);
                    Assert.True(toLeft.Name(tree).CompareTo(toLeft.Previous.Name(tree)) > 0);

                    toRight = toRight.Next;
                    toLeft = toLeft.Previous;
                }

                var leaves = new HashSet<ZFastTrieSortedSet<T, W>.Leaf>();
                var references = new HashSet<T>();

                int numberOfNodes = VisitNodes(tree, tree.Root, null, 0, nodes, leaves, references);
                Assert.Equal(2 * tree.Count - 1, numberOfNodes); // The amount of nodes is directly correlated with the tree size.
                Assert.Equal(tree.Count, leaves.Count); // The size of the tree is equal to the amount of leaves in the tree.

                int counter = 0;
                foreach (var leaf in leaves)
                {
                    if (references.Contains(leaf.Key))
                        counter++;
                }

                Assert.Equal(tree.Count - 1, counter);
            }

            Assert.Equal(0, nodes.Count);
        }

        private static int VisitNodes<T, W>(ZFastTrieSortedSet<T, W> tree, ZFastTrieSortedSet<T, W>.Node node,
                                     ZFastTrieSortedSet<T, W>.Node parent, int nameLength,
                                     HashSet<ZFastTrieSortedSet<T, W>.Node> nodes,
                                     HashSet<ZFastTrieSortedSet<T, W>.Leaf> leaves,
                                     HashSet<T> references) where T : IEquatable<T>
        {
            if (node == null)
                return 0;

            Assert.True(nameLength <= node.GetExtentLength(tree));

            var parentAsInternal = parent as ZFastTrieSortedSet<T, W>.Internal;
            if (parentAsInternal != null)
                Assert.True(parent.Extent(tree).Equals(node.Extent(tree).SubVector(0, parentAsInternal.ExtentLength)));

            if (node is ZFastTrieSortedSet<T, W>.Internal)
            {
                var leafNode = node.ReferencePtr as ZFastTrieSortedSet<T, W>.Leaf;
                Assert.NotNull(leafNode); // We ensure that internal node references are leaves. 

                Assert.True(references.Add(leafNode.Key));
                Assert.True(nodes.Remove(node));

                var handle = node.Handle(tree);

                var allNodes = tree.NodesTable.SelectMany(x => x.Value)
                                              .Select(x => x.Handle(tree));

                Assert.True(allNodes.Contains(handle));

                var internalNode = (ZFastTrieSortedSet<T, W>.Internal)node;
                int jumpLength = internalNode.GetJumpLength(tree);

                var jumpLeft = internalNode.Left;
                while (jumpLeft is ZFastTrieSortedSet<T, W>.Internal && jumpLength > ((ZFastTrieSortedSet<T, W>.Internal)jumpLeft).ExtentLength)
                    jumpLeft = ((ZFastTrieSortedSet<T, W>.Internal)jumpLeft).Left;

                Assert.Equal(internalNode.JumpLeftPtr, jumpLeft);

                var jumpRight = internalNode.Right;
                while (jumpRight is ZFastTrieSortedSet<T, W>.Internal && jumpLength > ((ZFastTrieSortedSet<T, W>.Internal)jumpRight).ExtentLength)
                    jumpRight = ((ZFastTrieSortedSet<T, W>.Internal)jumpRight).Right;

                Assert.Equal(internalNode.JumpRightPtr, jumpRight);

                return 1 + VisitNodes(tree, internalNode.Left, internalNode, internalNode.ExtentLength + 1, nodes, leaves, references)
                         + VisitNodes(tree, internalNode.Right, internalNode, internalNode.ExtentLength + 1, nodes, leaves, references);
            }
            else
            {
                var leafNode = node as ZFastTrieSortedSet<T, W>.Leaf;

                Assert.NotNull(leafNode);
                Assert.True(leaves.Add(leafNode)); // We haven't found this leaf somewhere else.
                Assert.Equal(leafNode.Name(tree).Count, leafNode.GetExtentLength(tree)); // This is a leaf, the extent is the key

                Assert.True(parent.ReferencePtr is ZFastTrieSortedSet<T, W>.Leaf); // We ensure that internal node references are leaves. 

                return 1;
            }
        }
    }
}
