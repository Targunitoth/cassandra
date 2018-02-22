//Quelle: https://github.com/kentliau/yet-another-tree-structure/blob/master/java/src/com/tree/TreeNode.java

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.blockchain;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.schema.TableMetadata;


public class TreeNode implements Iterable<TreeNode>
{

    private static TableMetadata metadata = null;
    public ByteBuffer data;
    public TreeNode parent;
    public List<TreeNode> children;
    public HashMap<ByteBuffer, ByteBuffer> orphans;
    public int orphanCounter = 0;

    public boolean isRoot()
    {
        return parent == null;
    }

    public boolean isLeaf()
    {
        return children.size() == 0;
    }

    private List<TreeNode> elementsIndex;

    public TreeNode(ByteBuffer data)
    {
        this.data = data;
        this.children = new LinkedList<TreeNode>();
        this.orphans = new HashMap<ByteBuffer, ByteBuffer>();
        this.elementsIndex = new LinkedList<TreeNode>();
        this.elementsIndex.add(this);
    }

    /*public TreeNode<ByteBuffer> addChild(T child) {
        TreeNode<ByteBuffer> childNode = new TreeNode<ByteBuffer>(child);
        childNode.parent = this;
        this.children.add(childNode);
        this.registerChildForSearch(childNode);
        return childNode;
    }*/

    public TreeNode addChildForParent(ByteBuffer child, ByteBuffer predecessor)
    {
        TreeNode parentNode = findTreeNode(predecessor);
        //Parent currently not in Database
        if (parentNode == null)
        {
            //Save as orphans
            orphans.put(predecessor, child);
            orphanCounter++;
            return null;
        }

        TreeNode childNode = new TreeNode(child);
        childNode.parent = parentNode;
        parentNode.children.add(childNode);
        parentNode.registerChildForSearch(childNode);
        return childNode;
    }

    public static void updatMetadata(TableMetadata md)
    {
        metadata = md;
    }

    public static boolean metadataSet()
    {
        return metadata != null;
    }

    public boolean areThereOrphans()
    {
        return orphanCounter > 0;
    }

    public void fixOrphans()
    {
        TreeNode parentNode;
        for (Map.Entry<ByteBuffer, ByteBuffer> entry : orphans.entrySet())
        {
            parentNode = findTreeNode(entry.getKey());
            if (parentNode != null)
            {
                addChildForParent(entry.getValue(), entry.getKey());
                orphanCounter--;
            }
        }
    }

    public int getLevel()
    {
        if (this.isRoot())
            return 0;
        else
            return parent.getLevel() + 1;
    }

    public HashMap<TreeNode, Integer> getLeafs()
    {
        HashMap<TreeNode, Integer> result = new HashMap<TreeNode, Integer>();
        for (TreeNode element : this.elementsIndex)
        {
            if (element.isLeaf())
            {
                result.put(element, element.getLevel());
            }
        }
        return result;
    }

    private void registerChildForSearch(TreeNode node)
    {
        elementsIndex.add(node);
        if (parent != null)
            parent.registerChildForSearch(node);
    }

    public TreeNode findTreeNode(ByteBuffer cmp)
    {
        if (cmp == null)
        {
            return null;
        }
        byte[] child = Arrays.copyOfRange(cmp.array(), cmp.position(), cmp.limit());
        for (TreeNode element : this.elementsIndex)
        {
            byte[] parent = Arrays.copyOfRange(element.data.array(), element.data.position(), element.data.limit());

            if (equal(child, parent))
                return element;
        }

        return null;
    }

    private boolean equal(byte[] child, byte[] parent)
    {
        if (child == null || parent == null)
        {
            return false;
        }

        if (child.length != parent.length)
        {
            return false;
        }
        if (child.equals(parent))
        {
            return true;
        }

        for (int i = 0; i < child.length; i++)
        {
            if (child[i] != parent[i])
            {
                return false;
            }
        }

        return true;
    }

    public String printTreeLeafs()
    {
        String result = "";
        Map<TreeNode, Integer> leafs = getLeafs();

        for (Map.Entry<TreeNode, Integer> entry : leafs.entrySet())
        {
            result += "Leaf: " + entry.getKey().toString() + " Depth: " + entry.getValue().toString();
            if (leafs.size() > 1)
            {
                result += "\n";
                System.out.println("New Tree:");
                printThisTrace(entry.getKey());
            }
        }
        result += " -> Current Orphan Count: " + orphanCounter;
        return result;
    }

    private void printThisTrace(TreeNode tn)
    {
        if (!tn.isRoot())
        {
            tn.printThisTrace(tn.parent);
        }
        System.out.println(tn.toString());
    }

    @Override
    public String toString()
    {
        return data != null ? FormatHelper.convertByteBufferToString((java.nio.ByteBuffer) data) : "[data null]";
    }

    @Override
    public Iterator<TreeNode> iterator()
    {
        TreeNodeIter iter = new TreeNodeIter(this);
        return iter;
    }

    public static TreeNode buildTree()
    {
        TreeNode tree = new TreeNode(BlockchainHandler.getNullBlock());
        UntypedResultSet rs = FormatHelper.executeQuery("SELECT blockchainid, predecessor FROM " + metadata.keyspace + "." + metadata.name + ";");
        for (UntypedResultSet.Row row : rs)
        {
            tree.addChildForParent(row.getBytes("blockchainid"), row.getBytes("predecessor"));
        }

        while (tree.areThereOrphans())
        {
            tree.fixOrphans();
        }
        return tree;
    }

    public LinkedList<TreeNode> getLongestPath()
    {
        LinkedList<TreeNode> result = new LinkedList<TreeNode>();
        int max = 0;
        for (Map.Entry<TreeNode, Integer> entry : getLeafs().entrySet())
        {
            if (entry.getValue() > max)
            {
                max = entry.getValue();
            }
        }
        for (Map.Entry<TreeNode, Integer> entry : getLeafs().entrySet())
        {
            if (entry.getValue() == max)
            {
                result.add(entry.getKey());
            }
        }
        return result;
    }
}
